# IIVC Life Skills Training Form Updates

## Overview
The IIVC Life Skills Training form structure has been updated in CommCare, requiring changes to our dbt models to handle both the old and new JSON formats while maintaining the same output structure for dashboards and reports.

## Changes Made

### Problem Statement
- The new form submissions use a different JSON structure with `membership_details` array instead of the previous `community_information` and `community_safe_space_participants_details` arrays
- New fields have been added: hard copy registration images and page counts
- We need backward compatibility to process both old and new form submissions

### Solution Approach
Instead of complex conditional logic, we created separate staging models for each format and merged them in the marts layer.

## New Files Created

### 1. `staging_life_skills_training_session_details_v2.sql`
- **Purpose**: Processes the new JSON format for session-level data
- **Key differences from v1**:
  - Filters for records with `membership_details` array
  - Extracts new fields: `hard_copy_registration_number_of_pages`, `hard_copies_registration_form` images
  - Calculates `total_participants_count` from membership array length
  - Does NOT expect community information fields (village_name, community_safe_space_name, etc.)

### 2. `staging_life_skills_training_participant_details_v2.sql` 
- **Purpose**: Processes the new JSON format for participant-level data
- **Key differences from v1**:
  - Extracts participant data from `membership_details` array instead of separate target group arrays
  - Uses field names: `member_full_names_first_middle_surname` and `member_gender`
  - Currently only handles `roc_club` target group (new format doesn't include community safe space structure)

## Updated Files

### 1. `life_skills_training_sessions.sql` (Mart Model)
- **Change**: Now unions data from both v1 and v2 staging models
- **New fields added**:
  - `hard_copy_pages_count`
  - `hard_copy_registration_images` 
  - `total_participants_count`
- **Backward compatibility**: Old records show NULL for new fields, new records show NULL for community fields

### 2. `life_skills_training_participants.sql` (Mart Model)
- **Change**: Simple union of both v1 and v2 staging participant models
- **Result**: Same output schema, handles both JSON formats seamlessly

## Data Mapping

### Old Format → New Format Field Mapping
| Old Format | New Format | Notes |
|------------|------------|-------|
| `community_information` → `village_name` | Not available | Shows NULL for new records |
| `community_information` → `community_safe_space_name` | Not available | Shows NULL for new records |
| `community_information` → `num_girls_in_safe_space` | Not available | Shows NULL for new records |
| Not available | `hard_copy_registration_number_of_pages` | Shows NULL for old records |
| Not available | `hard_copies_registration_form` images | Shows NULL for old records |
| Not available | `total_participants_count` | Shows NULL for old records |
| `membership_details` / `community_safe_space_participants_details` | `membership_details` | New unified array structure |

## Impact Assessment

### ✅ What Works
- **Backward compatibility**: All existing data continues to process
- **Same output schema**: Dashboards and reports require no changes
- **Clean separation**: Old and new logic are separate, easier to maintain
- **Extensible**: Easy to add more format versions in the future

### ⚠️ Limitations
- New format doesn't include community safe space participant structure yet
- Some fields are mutually exclusive between formats (community vs hard copy fields)

## Testing Recommendations

1. **Data Quality Check**: Verify both old and new records appear in final marts
2. **Field Validation**: Confirm new fields populate correctly for new submissions
3. **Dashboard Testing**: Ensure existing visualizations still work with the updated schema
4. **Count Verification**: Compare total participant counts between old and new calculation methods

## Future Considerations

- Monitor for additional form structure changes
- Consider archiving v1 models once all old data has been processed
- May need to create v3 models if community safe space structure is added to new format

---

**Date**: February 2026  
**Modified by**: Claude Code Assistant  
**Review needed**: MEL Team validation of output data quality