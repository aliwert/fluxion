from __future__ import annotations

import argparse
import base64
import binascii
import csv
import hashlib
import hmac
import importlib.machinery
import io
import posixpath
import re
import struct
import zipfile
import zlib
from dataclasses import dataclass
from email import policy
from email.errors import MessageError
from email.message import Message
from email.parser import Parser
from pathlib import Path
from typing import NoReturn

DISTRIBUTION = "fluxion"
SUPPORTED_PYTHON_VERSIONS = frozenset({"3.10", "3.11", "3.12", "3.13", "3.14"})
MACOS_TARGET = "macosx_11_0_arm64"
LINUX_TARGET = "manylinux_2_17_x86_64"
LINUX_ALIAS = "manylinux2014_x86_64"
LINUX_REQUIRED_PLATFORMS = frozenset({LINUX_TARGET, LINUX_ALIAS})
SIGNATURE_NAMES = ("RECORD.jws", "RECORD.p7s")

TAG_COMPONENT_RE = re.compile(r"[A-Za-z0-9_]+")
MACOS_PLATFORM_RE = re.compile(r"macosx_([0-9]+)_([0-9]+)_arm64")
MANYLINUX_X86_64_RE = re.compile(r"manylinux_[0-9]+_[0-9]+_x86_64")
SIZE_RE = re.compile(r"[0-9]+")
DRIVE_PATH_RE = re.compile(r"^[A-Za-z]:")
BASE64URL_RE = re.compile(r"[A-Za-z0-9_-]+")
IMPORTABLE_EXTENSION_SUFFIXES = tuple(
    suffix
    for suffix in importlib.machinery.EXTENSION_SUFFIXES
    if suffix.endswith(".so")
) or (".so",)

MAX_TAG_EXPANSIONS = 256
MAX_METADATA_BYTES = 4 * 1024 * 1024
MAX_RECORD_BYTES = 16 * 1024 * 1024
MAX_EXTENSION_BYTES = 128 * 1024 * 1024
ARCHIVE_ERRORS = (
    OSError,
    RuntimeError,
    NotImplementedError,
    EOFError,
    ValueError,
    zipfile.BadZipFile,
    zipfile.LargeZipFile,
    zlib.error,
)

ELF_CLASS_64 = 2
ELF_DATA_LITTLE_ENDIAN = 1
ELF_VERSION_CURRENT = 1
ELF_TYPE_SHARED = 3
EM_X86_64 = 62

MACHO_MAGIC_64_LITTLE = b"\xcf\xfa\xed\xfe"
MACHO_MAGIC_64_BIG = b"\xfe\xed\xfa\xcf"
MACHO_MAGIC_32 = {b"\xce\xfa\xed\xfe", b"\xfe\xed\xfa\xce"}
MACHO_FAT_MAGICS = {
    b"\xca\xfe\xba\xbe",
    b"\xbe\xba\xfe\xca",
    b"\xca\xfe\xba\xbf",
    b"\xbf\xba\xfe\xca",
}
MACHO_MAGIC_64 = 0xFEEDFACF
CPU_TYPE_ARM64 = 0x0100000C
MH_DYLIB = 6
MH_BUNDLE = 8


@dataclass(frozen=True)
class WheelFilename:
    version: str
    build_tag: str | None
    tags: frozenset[tuple[str, str, str]]


def fail(message: str) -> NoReturn:
    raise SystemExit(f"wheel check failed: {message}")


def parse_metadata(text: str, label: str) -> Message:
    try:
        message = Parser(policy=policy.default).parsestr(text)
    except (MessageError, ValueError) as error:
        fail(f"malformed {label}: {error}")
    if message.defects:
        defects = ", ".join(
            sorted({type(defect).__name__ for defect in message.defects})
        )
        fail(f"malformed {label}: {defects}")
    return message


def read_metadata(message: Message, key: str) -> list[str]:
    values = message.get_all(key)
    if values is None:
        return []
    return [str(value).strip() for value in values]


def expand_tag_component(component: str, label: str) -> tuple[str, ...]:
    values = component.split(".")
    if not values or any(not TAG_COMPONENT_RE.fullmatch(value) for value in values):
        fail(f"malformed {label} tag: {component!r}")
    if len(values) > MAX_TAG_EXPANSIONS:
        fail(f"{label} tag expands to too many values: {component!r}")
    return tuple(values)


def expand_tag(tag: str, label: str) -> frozenset[tuple[str, str, str]]:
    parts = tag.split("-")
    if len(parts) != 3:
        fail(f"malformed {label} tag: {tag!r}")
    python_tags = expand_tag_component(parts[0], f"{label} Python")
    abi_tags = expand_tag_component(parts[1], f"{label} ABI")
    platform_tags = expand_tag_component(parts[2], f"{label} platform")
    count = len(python_tags) * len(abi_tags) * len(platform_tags)
    if count > MAX_TAG_EXPANSIONS:
        fail(f"{label} tag expands to too many combinations: {tag!r}")
    return frozenset(
        (python_tag, abi_tag, platform_tag)
        for python_tag in python_tags
        for abi_tag in abi_tags
        for platform_tag in platform_tags
    )


def expected_platform(platform: str) -> str:
    if platform == "macos-arm64":
        return MACOS_TARGET
    if platform == "linux-x86_64":
        return LINUX_TARGET
    fail(f"unsupported platform: {platform}")


def platform_is_allowed(platform: str, platform_tag: str) -> bool:
    if platform == "macos-arm64":
        match = MACOS_PLATFORM_RE.fullmatch(platform_tag)
        return match is not None and int(match.group(1)) >= 11
    if platform == "linux-x86_64":
        return (
            platform_tag in LINUX_REQUIRED_PLATFORMS
            or MANYLINUX_X86_64_RE.fullmatch(platform_tag) is not None
        )
    return False


def validate_tag_set(
    tags: frozenset[tuple[str, str, str]], abi: str, platform: str
) -> None:
    if not tags:
        fail("wheel has no tags")
    target = expected_platform(platform)
    for python_tag, abi_tag, platform_tag in tags:
        if python_tag != abi:
            fail(f"unexpected Python tag {python_tag!r}, expected {abi!r}")
        if abi_tag != abi:
            fail(f"unexpected ABI tag {abi_tag!r}, expected {abi!r}")
        if not platform_is_allowed(platform, platform_tag):
            fail(f"unexpected platform tag {platform_tag!r} for target {platform!r}")
    if platform == "linux-x86_64":
        if not any(
            platform_tag in LINUX_REQUIRED_PLATFORMS for _, _, platform_tag in tags
        ):
            fail(
                f"wheel tags do not include required platform {LINUX_TARGET!r} "
                f"or alias {LINUX_ALIAS!r}"
            )
    elif not any(platform_tag == target for _, _, platform_tag in tags):
        fail(f"wheel tags do not include required platform {target!r}")


def parse_wheel_filename(
    wheel: Path, python_version: str, platform: str
) -> WheelFilename:
    if wheel.suffix != ".whl":
        fail(f"unexpected wheel filename: {wheel.name}")

    parts = wheel.stem.split("-")
    if len(parts) == 5:
        name, version, python_component, abi_component, platform_component = parts
        build_tag = None
    elif len(parts) == 6:
        (
            name,
            version,
            build_tag,
            python_component,
            abi_component,
            platform_component,
        ) = parts
        if not build_tag or not build_tag[0].isdigit():
            fail(f"invalid build tag in {wheel.name}")
    else:
        fail(f"unexpected wheel filename: {wheel.name}")

    if name != DISTRIBUTION:
        fail(f"unexpected distribution name in {wheel.name}")
    if not version:
        fail(f"missing version in {wheel.name}")

    abi = f"cp{python_version.replace('.', '')}"
    python_tags = set(expand_tag_component(python_component, "filename Python"))
    abi_tags = set(expand_tag_component(abi_component, "filename ABI"))
    if python_tags != {abi}:
        fail(f"expected Python tag {abi!r} in {wheel.name}")
    if abi_tags != {abi}:
        fail(f"expected ABI tag {abi!r} in {wheel.name}")

    tags = expand_tag(
        f"{python_component}-{abi_component}-{platform_component}",
        "filename",
    )
    validate_tag_set(tags, abi, platform)
    return WheelFilename(version=version, build_tag=build_tag, tags=tags)


def parse_wheel_tag(tag: str) -> tuple[str, str, str]:
    parts = tag.split("-")
    if len(parts) != 3:
        fail(f"malformed WHEEL tag: {tag!r}")
    if any(not TAG_COMPONENT_RE.fullmatch(part) for part in parts):
        fail(f"malformed WHEEL tag: {tag!r}")
    return parts[0], parts[1], parts[2]


def inspect_archive_names(names: list[str]) -> set[str]:
    seen: set[str] = set()
    seen_casefold: dict[str, str] = {}
    files: set[str] = set()
    for name in names:
        if not name:
            fail("archive contains an empty path")
        if any(ord(character) < 32 for character in name):
            fail(f"archive contains a control character in path: {name!r}")
        if "\\" in name:
            fail(f"archive contains a backslash in path: {name!r}")
        if name.startswith("/"):
            fail(f"archive contains an absolute path: {name!r}")
        if DRIVE_PATH_RE.match(name):
            fail(f"archive contains a drive path: {name!r}")
        if name in seen:
            fail(f"archive contains duplicate path: {name!r}")
        folded = name.casefold()
        previous = seen_casefold.get(folded)
        if previous is not None and previous != name:
            fail(f"archive contains case-insensitive duplicate path: {name!r}")
        seen.add(name)
        seen_casefold[folded] = name

        is_directory = name.endswith("/")
        path = name[:-1] if is_directory else name
        if not path:
            fail(f"archive contains an empty path: {name!r}")
        components = path.split("/")
        if any(component in {"", ".", ".."} for component in components):
            fail(f"archive contains an unsafe path: {name!r}")
        if posixpath.normpath(path) != path:
            fail(f"archive contains an unsafe path: {name!r}")
        if not is_directory:
            files.add(name)
    return files


def archive_dist_info_dirs(names: list[str]) -> set[str]:
    directories: set[str] = set()
    for name in names:
        path = name[:-1] if name.endswith("/") else name
        head, separator, _ = path.partition("/")
        if separator and head.endswith(".dist-info"):
            directories.add(head)
    return directories


def read_member_text(archive: zipfile.ZipFile, name: str, max_bytes: int) -> str:
    try:
        info = archive.getinfo(name)
    except KeyError:
        fail(f"archive is missing {name!r}")
    if info.file_size > max_bytes:
        fail(f"archive member {name!r} is unexpectedly large")
    try:
        data = archive.read(info)
    except ARCHIVE_ERRORS as error:
        fail(f"cannot read archive member {name!r}: {error}")
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError as error:
        fail(f"archive member {name!r} is not valid UTF-8: {error}")


def read_member_bytes(archive: zipfile.ZipFile, name: str, max_bytes: int) -> bytes:
    try:
        info = archive.getinfo(name)
    except KeyError:
        fail(f"archive is missing {name!r}")
    if info.file_size > max_bytes:
        fail(f"archive member {name!r} is unexpectedly large")
    try:
        return archive.read(info)
    except ARCHIVE_ERRORS as error:
        fail(f"cannot read archive member {name!r}: {error}")


def verify_record_entry(
    archive: zipfile.ZipFile, path: str, digest: str, size_text: str
) -> None:
    if not digest:
        fail(f"RECORD entry for {path!r} has no hash")
    if not SIZE_RE.fullmatch(size_text):
        fail(f"RECORD entry for {path!r} has an invalid size")

    algorithm, separator, encoded = digest.partition("=")
    if not separator or not algorithm or not encoded:
        fail(f"RECORD entry for {path!r} has a malformed hash")
    if "=" in encoded:
        fail(f"RECORD entry for {path!r} has a padded hash")
    if algorithm not in hashlib.algorithms_guaranteed:
        fail(f"RECORD entry for {path!r} uses unsupported hash {algorithm!r}")
    if not BASE64URL_RE.fullmatch(encoded):
        fail(f"RECORD entry for {path!r} has a non-URL-safe hash")

    try:
        padding = "=" * (-len(encoded) % 4)
        expected_digest = base64.b64decode(
            encoded + padding, altchars=b"-_", validate=True
        )
    except (binascii.Error, ValueError) as error:
        fail(f"RECORD entry for {path!r} has invalid base64: {error}")

    hasher = hashlib.new(algorithm)
    if hasher.digest_size < 32:
        fail(
            f"RECORD entry for {path!r} uses a hash weaker than sha256: "
            f"{algorithm!r}"
        )
    if len(expected_digest) != hasher.digest_size:
        fail(f"RECORD entry for {path!r} has an incorrect digest length")

    try:
        expected_size = int(size_text)
    except ValueError:
        fail(f"RECORD entry for {path!r} has an invalid size")
    try:
        info = archive.getinfo(path)
    except KeyError:
        fail(f"RECORD lists a missing file: {path!r}")

    actual_size = 0
    try:
        with archive.open(info) as member:
            while True:
                chunk = member.read(1024 * 1024)
                if not chunk:
                    break
                actual_size += len(chunk)
                hasher.update(chunk)
    except ARCHIVE_ERRORS as error:
        fail(f"cannot read archive member {path!r}: {error}")

    if actual_size != info.file_size:
        fail(f"archive size mismatch for {path!r}")
    if actual_size != expected_size:
        fail(f"RECORD size mismatch for {path!r}")
    if not hmac.compare_digest(hasher.digest(), expected_digest):
        fail(f"RECORD hash mismatch for {path!r}")


def validate_record(
    archive: zipfile.ZipFile, files: set[str], record_path: str
) -> None:
    record_text = read_member_text(archive, record_path, MAX_RECORD_BYTES)
    try:
        rows = list(csv.reader(io.StringIO(record_text), strict=True))
    except csv.Error as error:
        fail(f"malformed RECORD CSV: {error}")

    entries: dict[str, tuple[str, str]] = {}
    for line_number, row in enumerate(rows, start=1):
        if len(row) != 3:
            fail(f"malformed RECORD row {line_number}: expected three columns")
        path, digest, size = row
        if not path:
            fail(f"malformed RECORD row {line_number}: empty path")
        if path in entries:
            fail(f"RECORD contains duplicate path: {path!r}")
        entries[path] = (digest, size)

    if record_path not in entries:
        fail("RECORD does not list itself")
    if entries[record_path] != ("", ""):
        fail("RECORD must have an empty hash and size")

    dist_info = record_path.rsplit("/", 1)[0]
    signature_paths = {f"{dist_info}/{name}" for name in SIGNATURE_NAMES}
    for path in entries:
        if path == record_path:
            continue
        if path in signature_paths:
            fail(f"RECORD must not list signature file: {path!r}")
        if path not in files:
            fail(f"RECORD lists a missing file: {path!r}")

    for path in files:
        if path == record_path or path in signature_paths:
            continue
        if path not in entries:
            fail(f"RECORD is missing entry for: {path!r}")

    for path, (digest, size) in entries.items():
        if path == record_path or path in signature_paths:
            continue
        verify_record_entry(archive, path, digest, size)


def check_macho(data: bytes, extension: str) -> None:
    if len(data) < 4:
        fail(f"native extension is too short to be Mach-O: {extension}")
    magic = data[:4]
    if magic in MACHO_FAT_MAGICS:
        fail(f"universal Mach-O is not supported for this target: {extension}")
    if magic in MACHO_MAGIC_32:
        fail(f"native extension is not 64-bit Mach-O: {extension}")
    if magic == MACHO_MAGIC_64_LITTLE:
        endian = "<"
    elif magic == MACHO_MAGIC_64_BIG:
        endian = ">"
    else:
        fail(f"native extension is not Mach-O: {extension}")
    if len(data) < 32:
        fail(f"truncated Mach-O header: {extension}")

    magic_value, cpu_type, _cpu_subtype, file_type = struct.unpack_from(
        f"{endian}IiiI", data, 0
    )
    if magic_value != MACHO_MAGIC_64:
        fail(f"invalid Mach-O magic: {extension}")
    if cpu_type != CPU_TYPE_ARM64:
        fail(
            f"native extension has Mach-O CPU type {cpu_type:#x}, "
            f"expected ARM64: {extension}"
        )
    if file_type not in {MH_DYLIB, MH_BUNDLE}:
        fail(f"native extension has unexpected Mach-O file type: {extension}")

    ncmds, sizeofcmds = struct.unpack_from(f"{endian}II", data, 16)
    if 32 + sizeofcmds > len(data):
        fail(f"truncated Mach-O load commands: {extension}")
    if ncmds > sizeofcmds // 8:
        fail(f"inconsistent Mach-O load command count: {extension}")
    if ncmds == 0 and sizeofcmds != 0:
        fail(f"inconsistent Mach-O header: {extension}")


def check_elf(data: bytes, extension: str) -> None:
    if len(data) < 64:
        fail(f"native extension is too short to be ELF: {extension}")
    if data[:4] != b"\x7fELF":
        fail(f"native extension is not ELF: {extension}")
    if data[4] != ELF_CLASS_64:
        fail(f"native extension is not 64-bit ELF: {extension}")
    if data[5] != ELF_DATA_LITTLE_ENDIAN:
        fail(f"native extension is not little-endian ELF: {extension}")
    if data[6] != ELF_VERSION_CURRENT:
        fail(f"native extension has an invalid ELF version: {extension}")

    (
        file_type,
        machine,
        version,
        _entry,
        phoff,
        _shoff,
        _flags,
        ehsize,
        phentsize,
        phnum,
        _shentsize,
        _shnum,
        _shstrndx,
    ) = struct.unpack_from("<HHIQQQIHHHHHH", data, 16)
    if file_type != ELF_TYPE_SHARED:
        fail(f"native extension is not a shared ELF object: {extension}")
    if machine != EM_X86_64:
        fail(
            f"native extension has ELF machine {machine}, expected x86_64: {extension}"
        )
    if version != ELF_VERSION_CURRENT:
        fail(f"native extension has an invalid ELF header version: {extension}")
    if ehsize != 64:
        fail(f"native extension has an invalid ELF header size: {extension}")
    if phnum:
        if phentsize != 56:
            fail(
                f"native extension has an invalid ELF program header size: {extension}"
            )
        if phoff < ehsize:
            fail(
                f"native extension has an invalid ELF program header offset: {extension}"
            )
        if phoff + phnum * phentsize > len(data):
            fail(f"truncated ELF program header table: {extension}")


def expected_extension_suffix(python_version: str, platform: str) -> str:
    extension_abi = python_version.replace(".", "")
    if platform == "macos-arm64":
        return f".cpython-{extension_abi}-darwin.so"
    if platform == "linux-x86_64":
        return f".cpython-{extension_abi}-x86_64-linux-gnu.so"
    fail(f"unsupported platform: {platform}")


def check_extension(
    archive: zipfile.ZipFile, extension: str, python_version: str, platform: str
) -> None:
    expected = f"fluxion/_core{expected_extension_suffix(python_version, platform)}"
    if extension != expected:
        fail(
            f"native extension has an unexpected name: {extension!r}, "
            f"expected {expected!r}"
        )
    if not extension.endswith(IMPORTABLE_EXTENSION_SUFFIXES):
        fail(f"native extension has an unexpected suffix: {extension}")

    data = read_member_bytes(archive, extension, MAX_EXTENSION_BYTES)
    if platform == "macos-arm64":
        check_macho(data, extension)
    else:
        check_elf(data, extension)


def check_wheel(wheel: Path, python_version: str, platform: str, root: Path) -> None:
    if not wheel.is_file():
        fail(f"missing wheel: {wheel}")

    abi = f"cp{python_version.replace('.', '')}"
    parsed = parse_wheel_filename(wheel, python_version, platform)

    try:
        with zipfile.ZipFile(wheel) as archive:
            names = [info.filename for info in archive.infolist()]
            files = inspect_archive_names(names)

            metadata_names = sorted(
                name for name in files if name.endswith(".dist-info/METADATA")
            )
            wheel_names = sorted(
                name for name in files if name.endswith(".dist-info/WHEEL")
            )
            record_names = sorted(
                name for name in files if name.endswith(".dist-info/RECORD")
            )
            if len(metadata_names) != 1:
                fail("expected exactly one METADATA file")
            if len(wheel_names) != 1:
                fail("expected exactly one WHEEL file")
            if len(record_names) != 1:
                fail("expected exactly one RECORD file")

            metadata_name = metadata_names[0]
            wheel_name = wheel_names[0]
            record_path = record_names[0]
            dist_info = metadata_name.rsplit("/", 1)[0]
            if wheel_name != f"{dist_info}/WHEEL":
                fail("METADATA and WHEEL are in different dist-info directories")
            if record_path != f"{dist_info}/RECORD":
                fail("RECORD is in an unexpected dist-info directory")

            expected_dist_info = f"{DISTRIBUTION}-{parsed.version}.dist-info"
            if dist_info != expected_dist_info:
                fail(
                    f"unexpected dist-info directory {dist_info!r}, "
                    f"expected {expected_dist_info!r}"
                )
            dist_info_dirs = archive_dist_info_dirs(names)
            if dist_info_dirs != {dist_info}:
                fail(
                    f"unexpected .dist-info directories: {sorted(dist_info_dirs)}, "
                    f"expected {[dist_info]}"
                )

            metadata_text = read_member_text(archive, metadata_name, MAX_METADATA_BYTES)
            wheel_text = read_member_text(archive, wheel_name, MAX_METADATA_BYTES)
            metadata = parse_metadata(metadata_text, "METADATA")
            wheel_metadata = parse_metadata(wheel_text, "WHEEL")

            if read_metadata(metadata, "Name") != [DISTRIBUTION]:
                fail("unexpected package name metadata")
            if read_metadata(metadata, "Version") != [parsed.version]:
                fail("wheel filename and metadata versions differ")
            if read_metadata(metadata, "Requires-Python") != [">=3.10"]:
                fail("unexpected Requires-Python metadata")
            if read_metadata(metadata, "License") != ["MIT"]:
                fail("unexpected License metadata")
            if read_metadata(metadata, "Requires-Dist"):
                fail("wheel unexpectedly declares runtime dependencies")

            if read_metadata(wheel_metadata, "Root-Is-Purelib") != ["false"]:
                fail("wheel must not be marked purelib")

            wheel_tags: set[tuple[str, str, str]] = set()
            for tag in read_metadata(wheel_metadata, "Tag"):
                wheel_tags.add(parse_wheel_tag(tag))
            wheel_tag_set = frozenset(wheel_tags)
            validate_tag_set(wheel_tag_set, abi, platform)
            if not parsed.tags <= wheel_tag_set:
                missing = sorted(
                    f"{python}-{tag_abi}-{tag_platform}"
                    for python, tag_abi, tag_platform in parsed.tags - wheel_tag_set
                )
                fail(
                    "filename tags are not present in WHEEL metadata: "
                    + ", ".join(missing)
                )

            validate_record(archive, files, record_path)

            source_dir = root / "python" / "fluxion"
            expected_sources = {
                path.relative_to(root / "python").as_posix()
                for path in source_dir.rglob("*")
                if path.is_file()
                and (path.suffix in {".py", ".pyi"} or path.name == "py.typed")
            }
            missing_sources = expected_sources - files
            if missing_sources:
                fail(f"wheel is missing source files: {sorted(missing_sources)}")

            if any("__pycache__" in name.split("/") for name in files):
                fail("wheel contains Python cache files")
            if any(name.endswith(".pyc") for name in files):
                fail("wheel contains bytecode files")

            py_typed = "fluxion/py.typed"
            if py_typed not in files:
                fail("wheel is missing fluxion/py.typed")
            if archive.getinfo(py_typed).file_size != 0:
                fail("fluxion/py.typed must be an empty marker")

            extensions = [
                name
                for name in files
                if name.startswith("fluxion/_core.") and name.endswith((".so", ".pyd"))
            ]
            if len(extensions) != 1:
                fail(
                    f"expected exactly one native extension, found {sorted(extensions)}"
                )
            extension = extensions[0]
            check_extension(archive, extension, python_version, platform)
    except zipfile.BadZipFile as error:
        fail(f"invalid ZIP archive {wheel.name}: {error}")
    except ARCHIVE_ERRORS as error:
        fail(f"cannot inspect {wheel.name}: {error}")

    print(
        f"OK {wheel.name}: {abi}, {platform}, {len(names)} archive entries, "
        f"extension={extension}"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("wheels", nargs="+", type=Path)
    parser.add_argument("--python-version", required=True)
    parser.add_argument(
        "--platform",
        choices=["macos-arm64", "linux-x86_64"],
        required=True,
    )
    args = parser.parse_args()

    if args.python_version not in SUPPORTED_PYTHON_VERSIONS:
        fail(f"unsupported Python version: {args.python_version}")

    root = Path(__file__).resolve().parents[1]
    for wheel in args.wheels:
        check_wheel(wheel, args.python_version, args.platform, root)


if __name__ == "__main__":
    main()
