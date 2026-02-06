# How to create a new assessment

This guide provides a step-by-step walkthrough for data scientists and developers on how to create and integrate a new impact assessment into the worker.

## Architecture overview

The system is designed to be "pluggable". Each assessment is a self-contained Python class that encapsulates all of its domain-specific logic. A central "runner" is responsible for executing the correct assessment based on the job type, and an "orchestrator" handles the wiring.

The contract is simple:
1.  An **Assessment** class takes input data (`GeoDataFrame`, metadata, and a data `Repository`).
2.  Its `run()` method performs all calculations.
3.  It returns a dictionary of `pandas` or `geopandas` DataFrames.
4.  An **Adapter** function converts those DataFrames into structured Pydantic models for persistence.

---

## Step 1: Create the assessment class

First, create a new Python file for your assessment in the `worker/assessments/` directory.

**File:** `worker/assessments/my_assessment.py`

Inside this file, create a class that contains your logic. It must have an `__init__` method and a `run()` method.

**Template:**
```python
import logging
import geopandas as gpd
import pandas as pd
from worker.repositories.repository import Repository

logger = logging.getLogger(__name__)

class MyAssessment:
    """
    A brief description of what this assessment does.
    """

    def __init__(
        self,
        rlb_gdf: gpd.GeoDataFrame,
        metadata: dict,
        repository: Repository,
    ):
        """
        Initialize the assessment.

        Args:
            rlb_gdf: The Red Line Boundary GeoDataFrame for the development.
            metadata: A dictionary of metadata from the job (e.g., unique_ref).
            repository: The repository instance for querying data.
        """
        self.rlb_gdf = rlb_gdf
        self.metadata = metadata
        self.repository = repository
        # You can also initialize assessment-specific config here
        # from worker.config import MyAssessmentConfig
        # self.config = MyAssessmentConfig()

    def run(self) -> dict[str, pd.DataFrame | gpd.GeoDataFrame]:
        """
        Execute the core logic of the assessment.

        This method should be free of side effects (e.g., no file writing).

        Returns:
            A dictionary where keys are descriptive names and values are
            the resulting pandas or geopandas DataFrames.
        """
        logger.info("Running My Assessment")

        # --- Begin your domain logic here ---

        # 1. Load data using the repository
        # See docs/repository-design.md for details on querying.
        # example_layer = self.repository.load_spatial_layer(...)

        # 2. Perform calculations
        # result_df = self.rlb_gdf.copy()
        # result_df["my_calculation"] = 123.45

        # For this example, we'll just return the input
        result_df = self.rlb_gdf.copy()
        result_df["status"] = "processed"


        # --- End of domain logic ---

        logger.info("My Assessment complete")

        # 3. Return results as a dictionary of DataFrames
        return {
            "summary_results": result_df.drop(columns=["geometry"]),
            "spatial_results": result_df[["id", "geometry"]]
        }
```

## Step 2: Create the results adapter

The `run()` method returns raw DataFrames. The adapter is a function that converts this raw data into the final, structured Pydantic domain models that will be saved to the database.

Create a new file for your adapter in `worker/assessments/adapters/`.

**File:** `worker/assessments/adapters/my_assessment_adapter.py`

**Template:**
```python
import pandas as pd
from worker.models.domain import MyAssessmentResult # Assume you create this model

def to_domain_models(dataframes: dict) -> dict:
    """
    Convert the DataFrames from my_assessment.run() into Pydantic models.

    Args:
        dataframes: The dictionary of DataFrames returned by the assessment.

    Returns:
        A dictionary containing a list of Pydantic models, typically under
        the key "assessment_results".
    """
    summary_df = dataframes["summary_results"]

    # Convert each row of the DataFrame to a Pydantic model
    results = []
    for _, row in summary_df.iterrows():
        result_model = MyAssessmentResult(
            development_id=row["id"],
            status=row["status"],
            # ... map other fields
        )
        results.append(result_model)

    return {"assessment_results": results}
```
*(Note: You will also need to define the `MyAssessmentResult` Pydantic model in `worker/models/domain.py`)*

## Step 3: Register the new assessment

Now, you must "wire up" the new assessment so the application can find and run it. This involves two changes.

### 1. Register the assessment class

In `worker/runner/runner.py`, import your new assessment class and add it to the `ASSESSMENT_TYPES` dictionary. The key (e.g., `"my_assessment"`) is the string that will be used in job messages to request this assessment type.

**File:** `worker/runner/runner.py`
```python
# ... other imports
from worker.assessments.gcn import GcnAssessment
from worker.assessments.nutrient import NutrientAssessment
from worker.assessments.my_assessment import MyAssessment # 1. Import your class

logger = logging.getLogger(__name__)


ASSESSMENT_TYPES: dict[str, Type] = {
    "gcn": GcnAssessment,
    "nutrient": NutrientAssessment,
    "my_assessment": MyAssessment, # 2. Add your class to the dictionary
}

# ... rest of the file
```

### 2. Register the adapter function

In `worker/orchestrator.py`, import your new adapter and add a condition to call it when your assessment type is processed.

**File:** `worker/orchestrator.py`
```python
# ... other imports
from worker.assessments.adapters import gcn_adapter, nutrient_adapter, my_assessment_adapter # 1. Import adapter

# ... in JobOrchestrator.process_job() or _process_geometry_file()

        # ... after dataframes = run_assessment(...)

        if assessment_type == AssessmentType.NUTRIENT:
            domain_models = nutrient_adapter.to_domain_models(dataframes)
            assessment_results = domain_models["assessment_results"]
        elif assessment_type == AssessmentType.GCN:
            domain_models = gcn_adapter.to_domain_models(dataframes)
            assessment_results = domain_models["assessment_results"]
        # 2. Add a new block for your assessment
        elif assessment_type == AssessmentType.MY_ASSESSMENT: # (Assumes you add this to the Enum)
            domain_models = my_assessment_adapter.to_domain_models(dataframes)
            assessment_results = domain_models["assessment_results"]
        else:
            logger.error(f"Unsupported assessment type: {assessment_type.value}")
            return []
```
*(Note: This also requires adding `MY_ASSESSMENT = "my_assessment"` to the `AssessmentType` enum in `worker/models/enums.py`)*

## Step 4: Write unit tests

It is crucial to write unit tests for your assessment's domain logic. The pattern is to test the assessment class in isolation by providing it with sample data and a mock repository.

See existing tests like `tests/unit/assessments/test_gcn.py` or `tests/unit/assessments/test_nutrient.py` for excellent examples.

**Testing pattern:**
1.  Create sample `GeoDataFrame`s for your inputs.
2.  Create a mock `Repository` object (using `unittest.mock.Mock`).
3.  Configure the mock repository's `execute_query` method to return your sample data when called.
4.  Instantiate your assessment class with the sample data and mock repository.
5.  Call the `run()` method.
6.  Assert that the returned DataFrames have the expected structure and values.
