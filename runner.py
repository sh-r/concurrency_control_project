# Shika sr7463
# Khushboo kx2252
# Date: 6th December 2025

import sys
from repcrec import RepCRec

def run_test_block(lines: list[str], test_name: str) -> None:
    print()
    print(f"===== Running {test_name} =====")
    rec = RepCRec()
    for line in lines:
        rec.execute_line(line)

def main():
    if len(sys.argv) == 2:
        filename = sys.argv[1]
        with open(filename, "r") as f:
            all_lines = f.readlines()

        tests: list[list[str]] = []
        test_names: list[str] = []
        current_block: list[str] = []
        current_name = "Test"

        for line in all_lines:
            stripped = line.strip()
            if stripped.startswith("// Test"):
                if current_block:
                    tests.append(current_block)
                    test_names.append(current_name)
                    current_block = []
                current_name = stripped.lstrip("/").strip()
            current_block.append(line)

        if current_block:
            tests.append(current_block)
            test_names.append(current_name)

        for name, block in zip(test_names, tests):
            run_test_block(block, name)

    else:
        rec = RepCRec()
        for line in sys.stdin:
            rec.execute_line(line)

if __name__ == "__main__":
    main()