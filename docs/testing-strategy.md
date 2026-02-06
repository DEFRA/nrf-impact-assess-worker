This document outlines an analysis of the current unit testing suite and proposes a strategy for simplification and improvement. The goal is to focus testing effort on areas that provide the most value and confidence.

### Analysis of the current unit test suite

The unit test suite in `tests/unit/` is a mix of high-value tests that are essential for verifying application logic and lower-value tests that could be better implemented as integration tests.

#### 1. The "low-value" unit tests: `tests/unit/aws/**`

**⚠️ These tests have been deleted **

The tests for `S3Client` and `SQSClient` are of questionable value in their current form.

*   **Testing the mock, not the logic:** They use `moto` to create a mock AWS environment. Because the methods in `S3Client` and `SQSClient` are thin wrappers around `boto3`, the tests are mostly verifying that `boto3` works with `moto`, not that our application contains complex logic that needs verification.
*   **Doesn't test real-world integration:** These tests cannot check for IAM permission issues, correct bucket/queue naming, or network-related problems.
*   **Better alternatives exist:** The project is already configured to use **LocalStack**. An integration test that interacts with a running LocalStack container would be far more valuable, as it tests the actual configuration and interaction with an AWS-like API.

#### 2. The "high-value" unit tests

In contrast, many of the other unit tests are excellent examples of what unit tests should be:

*   **Business logic (`test_calculators.py`):** These are arguably the most valuable unit tests. They test pure functions with clear inputs and outputs (`calculate_land_use_uplift`, `apply_buffer`, etc.). They are fast, isolated, and ensure the core calculations of the application are correct.
*   **Domain logic (`assessments/test_gcn.py`):** These are also very high-value. They correctly test the complex workflow of an assessment in isolation by providing sample geodata and a mock repository. This allows the test to focus entirely on the assessment's internal logic without needing a database.
*   **Shared utilities (`spatial/test_operations.py`, `utils/test_spatial_utils.py`):** These are great for verifying that reusable spatial functions (`clip_gdf`, `majority_overlap`, etc.) work as expected on simple, predictable data.

#### 3. The "good practice" unit tests

The remaining tests are simple, fast, and represent good practice for maintaining a baseline of quality:

*   **Framework/orchestration (`executor/test_runner.py`):** These tests correctly mock the assessments to verify that the runner's own logic (e.g., looking up an assessment, calling it, validating its return type) works correctly.
*   **Data contracts (`test_models.py`, `test_job_models.py`):** These tests verify that Pydantic models behave as expected (e.g., validation, default values). They are cheap to write and protect the integrity of the application's data contracts.
*   **Stubs (`services/test_email.py`, `services/test_financial.py`):** These simply confirm that stubbed services raise a `NotImplementedError`, ensuring incomplete functionality isn't accidentally used.

### Proposed testing strategy

Based on this analysis, the following strategy is recommended to improve the focus and value of the testing suite:

1.  **KEEP** the high-value unit tests for:
    *   `calculators`
    *   `spatial` utilities
    *   `assessments` (domain logic)
    *   `executor` (framework logic)
    *   `models` (data contracts)

    These tests are fast, reliable, and essential for verifying the correctness of the core application.

2.  **DEPRECATE** the `moto`-based unit tests in `tests/unit/aws/`. They provide low value and do not offer sufficient confidence in the real-world integration with AWS services.

3.  **CREATE** a robust integration test suite in `tests/integration/` that uses the project's **LocalStack** configuration. These new tests should:
    *   Spin up a LocalStack container as part of the test setup.
    *   Test the `S3Client` and `SQSClient` by having them perform real actions against the LocalStack instance (e.g., create a bucket, upload a file, download it, send a message, receive it).
    *   This will provide much stronger confidence that the application's AWS integration is correctly configured and will function as expected in a deployed environment.
