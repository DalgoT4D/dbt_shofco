import re
from datetime import datetime

# Define test dates
test_dates = [
    "01/09/2024", "02/09/2024", "03/09/2024", "15/5/2000",
    "31/12/1999", "29/02/2020", "01/01/1950", "32/01/2024", "15/13/2024",
    "15-09-2024", "5/9/24", "5-9-24", "invalid_date", "07/07/2077"
]

# Define regex pattern for DD/MM/YYYY
# date_pattern = re.compile(r"^([0-2]?[0-9]|3[01])/([0]?[1-9]|1[0-2])/[1-2]\d{3}$")
date_pattern = re.compile(r"^(0?[1-9]|[12][0-9]|3[01])-(0?[1-9]|1[0-2])-\d{4}$")

# Function to validate and convert date
def validate_and_convert(date_str):
    match = date_pattern.match(date_str)
    if match:
        try:
            # Convert using strptime
            converted_date = datetime.strptime(date_str, "%d/%m/%Y").date()
            year = converted_date.year
            
            # Check year validity
            if 1950 <= year <= 2050:
                return f"✅ Valid: {converted_date}"
            else:
                return f"❌ Invalid Year: {converted_date}"
        except ValueError:
            return "❌ Conversion Error"
    else:
        return "❌ Regex Mismatch"

# Run tests
print("\nDate Validation Debugging\n" + "="*30)
for date in test_dates:
    result = validate_and_convert(date)
    print(f"Input: {date} → Output: {result}")