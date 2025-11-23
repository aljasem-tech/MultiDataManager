import re
import sys

def bump_version(file_path):
    with open(file_path, 'r') as f:
        content = f.read()

    # Regex to find version="X.Y.Z"
    version_pattern = r'version="(\d+)\.(\d+)\.(\d+)"'
    match = re.search(version_pattern, content)

    if not match:
        print("Version not found in setup.py")
        sys.exit(1)

    major, minor, patch = map(int, match.groups())
    
    # Increment minor version as per request (or patch? User said "minor version is increased by push automatically")
    # Usually push increments patch, but user specifically said "minor version is increased by push automatically"
    # I will follow user instructions: increment minor.
    new_minor = minor + 1
    new_patch = 0 # Reset patch when minor increments? Or just keep it? Standard is reset.
    
    new_version = f'{major}.{new_minor}.{new_patch}'
    
    new_content = re.sub(version_pattern, f'version="{new_version}"', content)

    with open(file_path, 'w') as f:
        f.write(new_content)

    print(f"Bumped version from {major}.{minor}.{patch} to {new_version}")

if __name__ == "__main__":
    bump_version('setup.py')
