from __future__ import annotations

import argparse
import re
import zipfile
from pathlib import Path


def fail(message: str) -> None:
    raise SystemExit(f"wheel check failed: {message}")


def read_metadata(lines: list[str], key: str) -> list[str]:
    prefix = f"{key}:"
    return [line[len(prefix) :].strip() for line in lines if line.startswith(prefix)]


def check_wheel(wheel: Path, python_version: str, platform: str, root: Path) -> None:
    if not wheel.is_file():
        fail(f"missing wheel: {wheel}")

    abi = f"cp{python_version.replace('.', '')}"
    parts = wheel.stem.split("-")
    if len(parts) < 5:
        fail(f"unexpected wheel filename: {wheel.name}")
    if parts[0] != "fluxion":
        fail(f"unexpected distribution name in {wheel.name}")
    if parts[2] != abi or parts[3] != abi:
        fail(f"expected {abi}-{abi} in {wheel.name}")

    with zipfile.ZipFile(wheel) as archive:
        names = set(archive.namelist())

        metadata_names = [name for name in names if name.endswith(".dist-info/METADATA")]
        wheel_names = [name for name in names if name.endswith(".dist-info/WHEEL")]
        if len(metadata_names) != 1 or len(wheel_names) != 1:
            fail("expected exactly one METADATA and one WHEEL file")

        metadata = archive.read(metadata_names[0]).decode().splitlines()
        wheel_metadata = archive.read(wheel_names[0]).decode().splitlines()

        if read_metadata(metadata, "Name") != ["fluxion"]:
            fail("unexpected package name metadata")
        versions = read_metadata(metadata, "Version")
        if versions != [parts[1]]:
            fail("wheel filename and metadata versions differ")
        if read_metadata(metadata, "Requires-Python") != [">=3.10"]:
            fail("unexpected Requires-Python metadata")
        if read_metadata(metadata, "License") != ["MIT"]:
            fail("unexpected License metadata")
        if read_metadata(metadata, "Requires-Dist"):
            fail("wheel unexpectedly declares runtime dependencies")

        if read_metadata(wheel_metadata, "Root-Is-Purelib") != ["false"]:
            fail("wheel must not be marked purelib")
        tags = read_metadata(wheel_metadata, "Tag")
        if not tags:
            fail("wheel has no Tag metadata")
        if not any(tag.startswith(f"{abi}-{abi}-") for tag in tags):
            fail(f"wheel tags do not include expected ABI {abi}")
        if platform == "macos-arm64":
            if not any("macosx_" in tag and tag.endswith("_arm64") for tag in tags):
                fail(f"wheel tags do not include macOS arm64: {tags}")
        elif platform == "linux-x86_64":
            if not any(
                ("manylinux" in tag or "linux" in tag) and "x86_64" in tag
                for tag in tags
            ):
                fail(f"wheel tags do not include Linux x86_64: {tags}")
        else:
            fail(f"unsupported platform: {platform}")

        source_dir = root / "python" / "fluxion"
        expected_sources = {
            path.relative_to(root / "python").as_posix()
            for path in source_dir.rglob("*")
            if path.is_file()
            and (path.suffix in {".py", ".pyi"} or path.name == "py.typed")
        }
        missing_sources = expected_sources - names
        if missing_sources:
            fail(f"wheel is missing source files: {sorted(missing_sources)}")

        if names != {name for name in names if "__pycache__" not in name}:
            fail("wheel contains Python cache files")
        if any(name.endswith(".pyc") for name in names):
            fail("wheel contains bytecode files")

        if "fluxion/py.typed" not in names:
            fail("wheel is missing fluxion/py.typed")
        if archive.getinfo("fluxion/py.typed").file_size != 0:
            fail("fluxion/py.typed must be an empty marker")

        extensions = [
            name
            for name in names
            if name.startswith("fluxion/_core.")
            and name.endswith((".so", ".pyd"))
        ]
        if len(extensions) != 1:
            fail(f"expected exactly one native extension, found {extensions}")
        extension = extensions[0]
        if f".cpython-{python_version.replace('.', '')}-" not in extension:
            fail(f"native extension has unexpected ABI: {extension}")
        if platform == "macos-arm64" and "darwin" not in extension:
            fail(f"native extension has unexpected platform: {extension}")
        if platform == "linux-x86_64" and "linux" not in extension:
            fail(f"native extension has unexpected platform: {extension}")

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

    if not re.fullmatch(r"3\.(?:10|11|12|13|14)", args.python_version):
        fail(f"unsupported Python version: {args.python_version}")

    root = Path(__file__).resolve().parents[1]
    for wheel in args.wheels:
        check_wheel(wheel, args.python_version, args.platform, root)


if __name__ == "__main__":
    main()
