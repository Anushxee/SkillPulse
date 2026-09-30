"""End-to-end validation entry point: python scripts/validate_project.py
Runs the full pipeline, exits 0 if all checks pass, non-zero otherwise —
suitable for CI or a pre-submission sanity check."""
import sys
import run_pipeline

if __name__ == "__main__":
    try:
        run_pipeline.main()
        print("\nvalidate_project.py: ALL CHECKS PASSED")
        sys.exit(0)
    except SystemExit as e:
        sys.exit(e.code)
    except AssertionError as e:
        print(f"\nvalidate_project.py: VALIDATION FAILED — {e}")
        sys.exit(1)
