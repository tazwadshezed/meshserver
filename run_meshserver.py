import subprocess
import os

def main():
    root = os.path.dirname(os.path.abspath(__file__))
    rundaq = os.path.join(root, "rundaq.py")
    emulator = os.path.join(root, "emulator.py")

    print("Running rundaq.py...")
    subprocess.Popen(["python3", rundaq])

    print("Running emulator.py...")
    subprocess.Popen(["python3", emulator])

if __name__ == "__main__":
    main()
