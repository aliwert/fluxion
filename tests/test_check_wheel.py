from __future__ import annotations

import base64
import hashlib
import struct
import subprocess
import sys
import warnings
import zipfile
from collections.abc import Callable
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check_wheel.py"
PYTHON_VERSION = "3.10"
ABI = "cp310"
DIST_INFO = "fluxion-0.1.0.dist-info"
MACOS_PLATFORM = "macosx_11_0_arm64"
MACOS_X86_64_PLATFORM = "macosx_11_0_x86_64"
LINUX_PLATFORM = "manylinux_2_17_x86_64"
LINUX_ALIAS = "manylinux2014_x86_64"
MUSLLINUX_PLATFORM = "musllinux_1_2_x86_64"
MACOS_EXTENSION = "fluxion/_core.cpython-310-darwin.so"
LINUX_EXTENSION = "fluxion/_core.cpython-310-x86_64-linux-gnu.so"
ZERO_DIGEST = base64.urlsafe_b64encode(b"\x00" * 32).rstrip(b"=").decode("ascii")


def macho_arm64() -> bytes:
    return struct.pack(
        "<IiiIIIII",
        0xFEEDFACF,
        0x0100000C,
        0,
        6,
        0,
        0,
        0,
        0,
    )


def macho_x86_64() -> bytes:
    return struct.pack(
        "<IiiIIIII",
        0xFEEDFACF,
        0x01000007,
        0,
        6,
        0,
        0,
        0,
        0,
    )


def elf_x86_64() -> bytes:
    return elf_header(62)


def elf_aarch64() -> bytes:
    return elf_header(183)


def elf_header(
    machine: int,
    *,
    version: int = 1,
    phoff: int = 0,
    phentsize: int = 0,
    phnum: int = 0,
    ehsize: int = 64,
) -> bytes:
    ident = b"\x7fELF" + bytes([2, 1, 1]) + b"\x00" * 9
    header = ident + struct.pack(
        "<HHIQQQIHHHHHH",
        3,
        machine,
        version,
        0,
        phoff,
        0,
        0,
        ehsize,
        phentsize,
        phnum,
        0,
        0,
        0,
    )
    assert len(header) == 64
    return header


def source_members() -> dict[str, bytes]:
    source_dir = ROOT / "python" / "fluxion"
    return {
        path.relative_to(ROOT / "python").as_posix(): path.read_bytes()
        for path in source_dir.rglob("*")
        if path.is_file()
        and (path.suffix in {".py", ".pyi"} or path.name == "py.typed")
    }


def metadata() -> bytes:
    return (
        b"Metadata-Version: 2.4\n"
        b"Name: fluxion\n"
        b"Version: 0.1.0\n"
        b"License: MIT\n"
        b"Requires-Python: >=3.10\n"
    )


def wheel_metadata(tags: tuple[str, ...]) -> bytes:
    lines = [
        "Wheel-Version: 1.0",
        "Generator: test",
        "Root-Is-Purelib: false",
    ]
    lines.extend(f"Tag: {tag}" for tag in tags)
    return ("\n".join(lines) + "\n").encode()


def build_record(
    members: dict[str, bytes], record_path: str, algorithm: str = "sha256"
) -> str:
    lines = []
    for path, data in sorted(members.items()):
        digest = base64.urlsafe_b64encode(hashlib.new(algorithm, data).digest()).rstrip(
            b"="
        )
        lines.append(f"{path},{algorithm}={digest.decode('ascii')},{len(data)}")
    lines.append(f"{record_path},,")
    return "\n".join(lines) + "\n"


def replace_first_digest(text: str) -> str:
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if ",sha256=" not in line:
            continue
        fields = line.split(",")
        fields[1] = f"sha256={ZERO_DIGEST}"
        lines[index] = ",".join(fields)
        break
    return "\n".join(lines) + "\n"


def replace_first_size(text: str) -> str:
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if line.endswith(",,"):
            continue
        fields = line.split(",")
        fields[2] = "999999"
        lines[index] = ",".join(fields)
        break
    return "\n".join(lines) + "\n"


def remove_first_entry(text: str) -> str:
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if not line.endswith(",,"):
            del lines[index]
            break
    return "\n".join(lines) + "\n"


def malformed_record(text: str) -> str:
    return text.replace("\n", "\nnot,enough\n", 1)


def record_self_digest(text: str) -> str:
    return text.replace(
        f"{DIST_INFO}/RECORD,,",
        f"{DIST_INFO}/RECORD,sha256={ZERO_DIGEST},0",
    )


def unclosed_csv_quote(text: str) -> str:
    return text.replace("\n", ',"\n', 1)


def duplicate_first_entry(text: str) -> str:
    lines = text.splitlines()
    for line in lines:
        if not line.endswith(",,"):
            return "\n".join([*lines, line]) + "\n"
    return text


def non_urlsafe_digest(text: str) -> str:
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if line.endswith(",,"):
            continue
        fields = line.split(",")
        algorithm, _, encoded = fields[1].partition("=")
        fields[1] = f"{algorithm}=+{encoded[1:]}"
        lines[index] = ",".join(fields)
        break
    return "\n".join(lines) + "\n"


def make_wheel(
    tmp_path: Path,
    *,
    platform: str = "macos-arm64",
    python_version: str = PYTHON_VERSION,
    filename_platform: str | None = None,
    wheel_tags: tuple[str, ...] | None = None,
    binary: bytes | None = None,
    extension: str | None = None,
    extra_members: dict[str, bytes] | None = None,
    unrecorded_members: dict[str, bytes] | None = None,
    omit: tuple[str, ...] = (),
    include_record: bool = True,
    record_transform: Callable[[str], str] | None = None,
    record_algorithm: str = "sha256",
    duplicate: tuple[str, ...] = (),
    filename: str | None = None,
) -> Path:
    abi = f"cp{python_version.replace('.', '')}"
    extension_abi = python_version.replace(".", "")
    if filename_platform is None:
        filename_platform = (
            MACOS_PLATFORM if platform == "macos-arm64" else LINUX_PLATFORM
        )
    if wheel_tags is None:
        wheel_tags = (f"{abi}-{abi}-{filename_platform}",)
    if binary is None:
        binary = macho_arm64() if platform == "macos-arm64" else elf_x86_64()
    if extension is None:
        extension = (
            f"fluxion/_core.cpython-{extension_abi}-darwin.so"
            if platform == "macos-arm64"
            else f"fluxion/_core.cpython-{extension_abi}-x86_64-linux-gnu.so"
        )

    members = source_members()
    members[f"{DIST_INFO}/METADATA"] = metadata()
    members[f"{DIST_INFO}/WHEEL"] = wheel_metadata(wheel_tags)
    members[extension] = binary
    if extra_members:
        members.update(extra_members)
    for name in omit:
        members.pop(name, None)

    record_path = f"{DIST_INFO}/RECORD"
    record = build_record(members, record_path, record_algorithm)
    if record_transform:
        record = record_transform(record)

    if filename is None:
        filename = f"fluxion-0.1.0-{abi}-{abi}-{filename_platform}.whl"
    wheel = tmp_path / filename
    with zipfile.ZipFile(wheel, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, data in members.items():
            archive.writestr(name, data)
        if include_record:
            archive.writestr(record_path, record.encode())
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            for name in duplicate:
                archive.writestr(name, members[name])
        for name, data in (unrecorded_members or {}).items():
            archive.writestr(name, data)
    return wheel


def run_validator(
    wheel: Path,
    platform: str = "macos-arm64",
    python_version: str = PYTHON_VERSION,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            str(wheel),
            "--python-version",
            python_version,
            "--platform",
            platform,
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def assert_valid(result: subprocess.CompletedProcess[str]) -> None:
    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("OK ")
    assert "wheel check failed" not in result.stderr


def assert_rejected(
    result: subprocess.CompletedProcess[str], fragment: str | None = None
) -> None:
    assert result.returncode != 0
    assert "wheel check failed:" in result.stderr
    assert "Traceback" not in result.stderr
    if fragment is not None:
        assert fragment in result.stderr


def test_valid_macos_arm64_wheel(tmp_path: Path) -> None:
    wheel = make_wheel(tmp_path)
    assert_valid(run_validator(wheel))


def test_valid_linux_manylinux_wheel(tmp_path: Path) -> None:
    wheel = make_wheel(tmp_path, platform="linux-x86_64")
    assert_valid(run_validator(wheel, platform="linux-x86_64"))


def test_valid_compressed_linux_tags(tmp_path: Path) -> None:
    wheel = make_wheel(
        tmp_path,
        platform="linux-x86_64",
        filename_platform=f"{LINUX_PLATFORM}.{LINUX_ALIAS}",
        wheel_tags=(f"{ABI}-{ABI}-{LINUX_PLATFORM}", f"{ABI}-{ABI}-{LINUX_ALIAS}"),
    )
    assert_valid(run_validator(wheel, platform="linux-x86_64"))


def test_valid_multiple_macos_tags(tmp_path: Path) -> None:
    wheel = make_wheel(
        tmp_path,
        filename_platform=f"{MACOS_PLATFORM}.macosx_12_0_arm64",
        wheel_tags=(
            f"{ABI}-{ABI}-{MACOS_PLATFORM}",
            f"{ABI}-{ABI}-macosx_12_0_arm64",
        ),
    )
    assert_valid(run_validator(wheel))


def test_valid_sha512_record(tmp_path: Path) -> None:
    wheel = make_wheel(
        tmp_path,
        platform="linux-x86_64",
        record_algorithm="sha512",
    )
    assert_valid(run_validator(wheel, platform="linux-x86_64"))


def test_valid_signature_files_are_not_recorded(tmp_path: Path) -> None:
    wheel = make_wheel(
        tmp_path,
        unrecorded_members={
            f"{DIST_INFO}/RECORD.jws": b"signature",
            f"{DIST_INFO}/RECORD.p7s": b"signature",
        },
    )
    assert_valid(run_validator(wheel))


def test_rejects_arm64_macho_with_x86_64_filename(tmp_path: Path) -> None:
    wheel = make_wheel(
        tmp_path,
        filename_platform=MACOS_X86_64_PLATFORM,
        wheel_tags=(f"{ABI}-{ABI}-{MACOS_X86_64_PLATFORM}",),
    )
    assert_rejected(run_validator(wheel))


def test_rejects_x86_64_macho_in_arm64_wheel(tmp_path: Path) -> None:
    wheel = make_wheel(tmp_path, binary=macho_x86_64())
    assert_rejected(run_validator(wheel), "Mach-O CPU type")


def test_rejects_musllinux_as_manylinux(tmp_path: Path) -> None:
    wheel = make_wheel(
        tmp_path,
        platform="linux-x86_64",
        filename_platform=MUSLLINUX_PLATFORM,
        wheel_tags=(f"{ABI}-{ABI}-{MUSLLINUX_PLATFORM}",),
    )
    assert_rejected(run_validator(wheel, platform="linux-x86_64"))


def test_rejects_aarch64_elf_in_x86_64_wheel(tmp_path: Path) -> None:
    wheel = make_wheel(
        tmp_path,
        platform="linux-x86_64",
        binary=elf_aarch64(),
    )
    assert_rejected(run_validator(wheel, platform="linux-x86_64"), "ELF machine")


def test_rejects_filename_wheel_tag_mismatch(tmp_path: Path) -> None:
    wheel = make_wheel(
        tmp_path,
        wheel_tags=("cp310-cp310-macosx_12_0_arm64",),
    )
    assert_rejected(run_validator(wheel))


def test_rejects_incompatible_python_abi(tmp_path: Path) -> None:
    wheel = make_wheel(
        tmp_path,
        filename="fluxion-0.1.0-cp311-cp311-macosx_11_0_arm64.whl",
    )
    assert_rejected(run_validator(wheel), "expected Python tag")


def test_rejects_stale_record_hash(tmp_path: Path) -> None:
    wheel = make_wheel(tmp_path, record_transform=replace_first_digest)
    assert_rejected(run_validator(wheel), "RECORD hash mismatch")


def test_rejects_incorrect_record_size(tmp_path: Path) -> None:
    wheel = make_wheel(tmp_path, record_transform=replace_first_size)
    assert_rejected(run_validator(wheel), "RECORD size mismatch")


def test_rejects_missing_record_entry(tmp_path: Path) -> None:
    wheel = make_wheel(tmp_path, record_transform=remove_first_entry)
    assert_rejected(run_validator(wheel), "RECORD is missing entry")


def test_rejects_malformed_zip(tmp_path: Path) -> None:
    wheel = tmp_path / "fluxion-0.1.0-cp310-cp310-macosx_11_0_arm64.whl"
    wheel.write_text("not a zip archive")
    assert_rejected(run_validator(wheel), "invalid ZIP archive")


def test_rejects_duplicate_archive_paths(tmp_path: Path) -> None:
    wheel = make_wheel(tmp_path, duplicate=("fluxion/__init__.py",))
    assert_rejected(run_validator(wheel), "duplicate path")


@pytest.mark.parametrize(
    "unsafe_path",
    [
        "../escape.py",
        "/absolute.py",
        "fluxion\\bad.py",
        "fluxion/../bad.py",
        "fluxion//bad.py",
    ],
)
def test_rejects_unsafe_archive_paths(tmp_path: Path, unsafe_path: str) -> None:
    wheel = make_wheel(
        tmp_path,
        unrecorded_members={unsafe_path: b"unsafe"},
    )
    assert_rejected(run_validator(wheel))


@pytest.mark.parametrize(
    "missing_name",
    [
        f"{DIST_INFO}/METADATA",
        f"{DIST_INFO}/WHEEL",
    ],
)
def test_rejects_missing_metadata(tmp_path: Path, missing_name: str) -> None:
    wheel = make_wheel(tmp_path, omit=(missing_name,))
    assert_rejected(run_validator(wheel))


def test_rejects_missing_record(tmp_path: Path) -> None:
    wheel = make_wheel(tmp_path, include_record=False)
    assert_rejected(run_validator(wheel))


def test_rejects_record_self_hash(tmp_path: Path) -> None:
    wheel = make_wheel(tmp_path, record_transform=record_self_digest)
    assert_rejected(run_validator(wheel), "RECORD must have an empty hash and size")


def test_rejects_unclosed_record_quote(tmp_path: Path) -> None:
    wheel = make_wheel(tmp_path, record_transform=unclosed_csv_quote)
    assert_rejected(run_validator(wheel), "malformed RECORD CSV")


def test_rejects_duplicate_record_entries(tmp_path: Path) -> None:
    wheel = make_wheel(tmp_path, record_transform=duplicate_first_entry)
    assert_rejected(run_validator(wheel), "duplicate path")


@pytest.mark.parametrize(
    "platform_tag",
    [
        "linux_x86_64",
        "manylinux_2_17_aarch64",
        "manylinux_2_28_x86_64",
    ],
)
def test_rejects_linux_platform_tags_without_required_target(
    tmp_path: Path, platform_tag: str
) -> None:
    wheel = make_wheel(
        tmp_path,
        platform="linux-x86_64",
        filename_platform=platform_tag,
        wheel_tags=(f"{ABI}-{ABI}-{platform_tag}",),
    )
    assert_rejected(run_validator(wheel, platform="linux-x86_64"))


def test_rejects_missing_native_extension(tmp_path: Path) -> None:
    wheel = make_wheel(tmp_path, omit=(MACOS_EXTENSION,))
    assert_rejected(run_validator(wheel), "expected exactly one native extension")


def test_rejects_malformed_record_csv(tmp_path: Path) -> None:
    wheel = make_wheel(tmp_path, record_transform=malformed_record)
    assert_rejected(run_validator(wheel), "malformed RECORD row")


def test_rejects_invalid_wheel_filename(tmp_path: Path) -> None:
    wheel = make_wheel(
        tmp_path,
        filename="fluxion-0.1.0-cp310-cp310.whl",
    )
    assert_rejected(run_validator(wheel), "unexpected wheel filename")


def test_rejects_invalid_tag_metadata(tmp_path: Path) -> None:
    wheel = make_wheel(
        tmp_path,
        wheel_tags=("cp310-cp310",),
    )
    assert_rejected(run_validator(wheel), "malformed WHEEL tag")


def test_rejects_signature_listed_in_record(tmp_path: Path) -> None:
    wheel = make_wheel(
        tmp_path,
        extra_members={f"{DIST_INFO}/RECORD.jws": b"signature"},
    )
    assert_rejected(run_validator(wheel), "RECORD must not list signature file")


@pytest.mark.parametrize("python_version", ["3.10", "3.11", "3.12", "3.13", "3.14"])
@pytest.mark.parametrize("platform", ["macos-arm64", "linux-x86_64"])
def test_accepts_canonical_extension_names(
    tmp_path: Path, python_version: str, platform: str
) -> None:
    wheel = make_wheel(tmp_path, python_version=python_version, platform=platform)
    assert_valid(
        run_validator(wheel, platform=platform, python_version=python_version)
    )


@pytest.mark.parametrize(
    "extension",
    [
        "fluxion/_core.cpython-310-x86_64-darwin.so",
        "fluxion/_core.cpython-310-arm64-darwin.so",
        "fluxion/_core.cpython-310-darwin.pyd",
    ],
)
def test_rejects_noncanonical_macos_extension_names(
    tmp_path: Path, extension: str
) -> None:
    wheel = make_wheel(tmp_path, extension=extension)
    assert_rejected(run_validator(wheel), "unexpected name")


@pytest.mark.parametrize(
    "extension",
    [
        "fluxion/_core.cpython-310-x86_64-linux.so",
        "fluxion/_core.cpython-310-x86_64-linux-gnux.so",
        "fluxion/_core.cpython-310-aarch64-linux-gnu.so",
    ],
)
def test_rejects_noncanonical_linux_extension_names(
    tmp_path: Path, extension: str
) -> None:
    wheel = make_wheel(tmp_path, platform="linux-x86_64", extension=extension)
    assert_rejected(run_validator(wheel, platform="linux-x86_64"), "unexpected name")


@pytest.mark.parametrize("algorithm", ["md5", "sha1", "sha224"])
def test_rejects_weak_record_hashes(tmp_path: Path, algorithm: str) -> None:
    wheel = make_wheel(
        tmp_path,
        platform="linux-x86_64",
        record_algorithm=algorithm,
    )
    assert_rejected(
        run_validator(wheel, platform="linux-x86_64"), "weaker than sha256"
    )


def test_valid_sha256_record(tmp_path: Path) -> None:
    wheel = make_wheel(tmp_path, platform="linux-x86_64", record_algorithm="sha256")
    assert_valid(run_validator(wheel, platform="linux-x86_64"))


def test_valid_linux_alias_only_wheel(tmp_path: Path) -> None:
    wheel = make_wheel(
        tmp_path,
        platform="linux-x86_64",
        filename_platform=LINUX_ALIAS,
        wheel_tags=(f"{ABI}-{ABI}-{LINUX_ALIAS}",),
    )
    assert_valid(run_validator(wheel, platform="linux-x86_64"))


def test_valid_linux_target_with_newer_manylinux(tmp_path: Path) -> None:
    newer = "manylinux_2_28_x86_64"
    wheel = make_wheel(
        tmp_path,
        platform="linux-x86_64",
        filename_platform=f"{LINUX_PLATFORM}.{newer}",
        wheel_tags=(f"{ABI}-{ABI}-{LINUX_PLATFORM}", f"{ABI}-{ABI}-{newer}"),
    )
    assert_valid(run_validator(wheel, platform="linux-x86_64"))


def test_rejects_newer_manylinux_without_target(tmp_path: Path) -> None:
    newer = "manylinux_2_28_x86_64"
    wheel = make_wheel(
        tmp_path,
        platform="linux-x86_64",
        filename_platform=newer,
        wheel_tags=(f"{ABI}-{ABI}-{newer}",),
    )
    assert_rejected(
        run_validator(wheel, platform="linux-x86_64"), "required platform"
    )


def test_rejects_wrong_arch_manylinux_with_target(tmp_path: Path) -> None:
    wheel = make_wheel(
        tmp_path,
        platform="linux-x86_64",
        filename_platform=f"{LINUX_PLATFORM}.manylinux_2_17_aarch64",
        wheel_tags=(
            f"{ABI}-{ABI}-{LINUX_PLATFORM}",
            f"{ABI}-{ABI}-manylinux_2_17_aarch64",
        ),
    )
    assert_rejected(
        run_validator(wheel, platform="linux-x86_64"), "unexpected platform tag"
    )


def test_accepts_metadata_body_with_header_like_lines(tmp_path: Path) -> None:
    custom = metadata() + (
        b"\nLong description.\n"
        b"Name: evil\n"
        b"Version: 9.9.9\n"
        b"Requires-Dist: numpy\n"
    )
    wheel = make_wheel(
        tmp_path,
        extra_members={f"{DIST_INFO}/METADATA": custom},
    )
    assert_valid(run_validator(wheel))


def test_accepts_folded_requires_python(tmp_path: Path) -> None:
    custom = (
        b"Metadata-Version: 2.4\n"
        b"Name: fluxion\n"
        b"Version: 0.1.0\n"
        b"License: MIT\n"
        b"Requires-Python: >=3.10\n"
        b"\t\n"
    )
    wheel = make_wheel(
        tmp_path,
        extra_members={f"{DIST_INFO}/METADATA": custom},
    )
    assert_valid(run_validator(wheel))


def test_rejects_folded_requires_python_range(tmp_path: Path) -> None:
    custom = (
        b"Metadata-Version: 2.4\n"
        b"Name: fluxion\n"
        b"Version: 0.1.0\n"
        b"License: MIT\n"
        b"Requires-Python: >=3.10,\n"
        b" <3.15\n"
    )
    wheel = make_wheel(
        tmp_path,
        extra_members={f"{DIST_INFO}/METADATA": custom},
    )
    assert_rejected(run_validator(wheel), "unexpected Requires-Python metadata")


def test_rejects_duplicate_metadata_header(tmp_path: Path) -> None:
    custom = (
        b"Metadata-Version: 2.4\n"
        b"Name: fluxion\n"
        b"Name: evil\n"
        b"Version: 0.1.0\n"
        b"License: MIT\n"
        b"Requires-Python: >=3.10\n"
    )
    wheel = make_wheel(
        tmp_path,
        extra_members={f"{DIST_INFO}/METADATA": custom},
    )
    assert_rejected(run_validator(wheel), "unexpected package name metadata")


def test_rejects_missing_required_metadata(tmp_path: Path) -> None:
    custom = (
        b"Metadata-Version: 2.4\n"
        b"Name: fluxion\n"
        b"Version: 0.1.0\n"
        b"Requires-Python: >=3.10\n"
    )
    wheel = make_wheel(
        tmp_path,
        extra_members={f"{DIST_INFO}/METADATA": custom},
    )
    assert_rejected(run_validator(wheel), "unexpected License metadata")


def test_rejects_malformed_metadata(tmp_path: Path) -> None:
    custom = (
        b"Metadata-Version: 2.4\n"
        b"Name fluxion\n"
        b"Version: 0.1.0\n"
        b"License: MIT\n"
        b"Requires-Python: >=3.10\n"
    )
    wheel = make_wheel(
        tmp_path,
        extra_members={f"{DIST_INFO}/METADATA": custom},
    )
    assert_rejected(run_validator(wheel), "malformed METADATA")


def test_rejects_inconsistent_macho_load_commands(tmp_path: Path) -> None:
    binary = struct.pack(
        "<IiiIIIII",
        0xFEEDFACF,
        0x0100000C,
        0,
        6,
        999,
        999,
        0,
        0,
    )
    wheel = make_wheel(tmp_path, binary=binary)
    assert_rejected(run_validator(wheel), "load command")


def test_rejects_truncated_macho_header(tmp_path: Path) -> None:
    wheel = make_wheel(tmp_path, binary=macho_arm64()[:16])
    assert_rejected(run_validator(wheel), "truncated Mach-O header")


def test_rejects_elf_with_invalid_header_version(tmp_path: Path) -> None:
    wheel = make_wheel(
        tmp_path,
        platform="linux-x86_64",
        binary=elf_header(62, version=0),
    )
    assert_rejected(
        run_validator(wheel, platform="linux-x86_64"), "ELF header version"
    )


def test_rejects_elf_with_out_of_bounds_program_headers(tmp_path: Path) -> None:
    wheel = make_wheel(
        tmp_path,
        platform="linux-x86_64",
        binary=elf_header(62, phoff=99999, phentsize=56, phnum=3),
    )
    assert_rejected(
        run_validator(wheel, platform="linux-x86_64"), "program header"
    )


def test_rejects_truncated_elf_header(tmp_path: Path) -> None:
    wheel = make_wheel(
        tmp_path,
        platform="linux-x86_64",
        binary=elf_x86_64()[:63],
    )
    assert_rejected(
        run_validator(wheel, platform="linux-x86_64"), "too short to be ELF"
    )


def test_rejects_compressed_wheel_tag(tmp_path: Path) -> None:
    wheel = make_wheel(
        tmp_path,
        filename_platform=f"{MACOS_PLATFORM}.macosx_12_0_arm64",
        wheel_tags=(f"{ABI}-{ABI}-{MACOS_PLATFORM}.macosx_12_0_arm64",),
    )
    assert_rejected(run_validator(wheel), "malformed WHEEL tag")


def test_rejects_non_urlsafe_record_digest(tmp_path: Path) -> None:
    wheel = make_wheel(tmp_path, record_transform=non_urlsafe_digest)
    assert_rejected(run_validator(wheel), "non-URL-safe")


def test_accepts_urlsafe_record_digest(tmp_path: Path) -> None:
    wheel = make_wheel(tmp_path)
    with zipfile.ZipFile(wheel) as archive:
        record = archive.read(f"{DIST_INFO}/RECORD").decode()
    py_typed_line = next(
        line for line in record.splitlines() if line.startswith("fluxion/py.typed,")
    )
    digest = py_typed_line.split(",")[1]
    assert "-" in digest and "_" in digest
    assert_valid(run_validator(wheel))


def test_rejects_multiple_dist_info_directories(tmp_path: Path) -> None:
    wheel = make_wheel(
        tmp_path,
        extra_members={"evil-1.0.dist-info/foo.txt": b"evil"},
    )
    assert_rejected(run_validator(wheel), "unexpected .dist-info directories")


def test_rejects_case_insensitive_archive_paths(tmp_path: Path) -> None:
    wheel = make_wheel(
        tmp_path,
        extra_members={"Fluxion/__init__.py": b"x"},
    )
    assert_rejected(run_validator(wheel), "case-insensitive duplicate path")
