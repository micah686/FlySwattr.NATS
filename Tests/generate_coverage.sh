#!/bin/bash

# Define directories
TESTS_DIR="Tests"
REPORTS_DIR="$TESTS_DIR/Reports"
UNIT_TESTS_PROJ="$TESTS_DIR/UnitTests/UnitTests.csproj"
INTEGRATION_TESTS_PROJ="$TESTS_DIR/IntegrationTests/IntegrationTests.csproj"

# Clean previous reports and results
echo "Cleaning up previous reports and results..."
rm -rf "$REPORTS_DIR"
find "$TESTS_DIR" -type d -name "TestResults" -exec rm -rf {} +

# Track overall success
EXIT_CODE=0

# Run Unit Tests
echo "Running Unit Tests with coverage..."
dotnet run --project "$UNIT_TESTS_PROJ" --coverage --coverage-output-format Cobertura
if [ $? -ne 0 ]; then
    echo "Unit Tests failed!"
    EXIT_CODE=1
fi

# Run Integration Tests with Retry logic for flaky Docker ports
echo "Running Integration Tests with coverage..."
INTEGRATION_SUCCESS=false
MAX_RETRIES=3
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    RETRY_COUNT=$((RETRY_COUNT+1))
    echo "Attempt $RETRY_COUNT of $MAX_RETRIES..."
    
    dotnet run --project "$INTEGRATION_TESTS_PROJ" --coverage --coverage-output-format Cobertura
    
    if [ $? -eq 0 ]; then
        INTEGRATION_SUCCESS=true
        break
    else
        echo "Integration Tests failed on attempt $RETRY_COUNT."
        if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
            echo "Retrying in 5 seconds..."
            sleep 5
            # Optional: Clean up TestResults for this project specifically before retry if needed, 
            # but usually dotnet run overwrites or appends cleanly. 
            # For coverage merging, we might get duplicate files if we don't clean, 
            # but TUnit/Coverlet usually handles unique IDs.
            # To be safe and clean, let's remove the specific result if we can, but simple retry is often enough.
        fi
    fi
done

if [ "$INTEGRATION_SUCCESS" = false ]; then
    echo "Integration Tests failed after $MAX_RETRIES attempts!"
    EXIT_CODE=1
fi

# Generate Report
echo "Generating HTML Coverage Report..."
reportgenerator -reports:"$TESTS_DIR/**/TestResults/*.cobertura.xml" -targetdir:"$REPORTS_DIR" -reporttypes:Html

echo "Done! Coverage report generated in $REPORTS_DIR/index.html"
exit $EXIT_CODE
