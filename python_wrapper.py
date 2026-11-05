import subprocess
import sys

with open(
    r"C:\Users\navya\Desktop\ecommerce-data-platform\python_worker_log.txt", "a"
) as f:
    f.write(f"ARGS: {sys.argv}\n")
    try:
        import pyspark

        f.write(f"PYSPARK_FILE: {pyspark.__file__}\n")
    except Exception as e:
        f.write(f"PYSPARK_FILE_ERR: {e}\n")
    f.flush()

    try:
        # Run the actual python executable
        actual_python = (
            r"C:\Users\navya\Desktop\ecommerce-data-platform\venv\Scripts\python.exe"
        )

        # We need to run it and pipe the output to both our log and the original stdout/stderr
        result = subprocess.run([actual_python] + sys.argv[1:], capture_output=True)

        f.write(f"STDOUT:\n{result.stdout.decode('utf-8', 'replace')}\n")
        f.write(f"STDERR:\n{result.stderr.decode('utf-8', 'replace')}\n")
        f.write(f"EXIT CODE: {result.returncode}\n")

        sys.stdout.buffer.write(result.stdout)
        sys.stderr.buffer.write(result.stderr)

        sys.exit(result.returncode)
    except Exception as e:
        f.write(f"EXCEPTION: {e}\n")
        sys.exit(1)
