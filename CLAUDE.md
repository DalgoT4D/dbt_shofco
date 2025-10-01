# CLAUDE.md - DBT Project Reference

## Staging SL Case Table Overview

The `staging_sl_case_table` is a large table (800+ columns) containing all SL (Skills & Livelihoods) program data.

### Key Field Categories:

#### Demographics
- `pp_fullname` - Participant full name  
- `sex` - Gender (coalesced from multiple sources)
- `age` - Age (coalesced from multiple sources)
- `pp_unique_id` - Unique participant ID
- `county` - County (coalesced from multiple sources)
- `pp_shofco_subcounty` - Subcounty
- `pp_ahofco_ward` - Ward

#### Daycare Fields
- `do_you_have_children` - Has children (coalesced)
- `number_of_children` - Number of children (coalesced)
- `children_under_4` - Children under 4 (coalesced)
- `children_with_disabilities` - Children with disabilities (coalesced)  
- `children_in_need_of_daycare_support` - Daycare support details (coalesced)
- `children_inneed_daycare_uf` - Alternative daycare field

#### Dignity Kit Fields
- `experienced_days_where_you_had_nothing_sr` - Experienced days without basics (SR)
- `experienced_days_where_you_had_nothing_uf` - Experienced days without basics (UF)
- `miss_school_due_to_lack_hygiene_products_sr` - Missed school due to hygiene (SR)
- `miss_school_due_to_lack_hygiene_products_uf` - Missed school due to hygiene (UF)
- `challenges_accessing_personal_hygiene_items` - Hygiene challenges (coalesced)
- `dignity_kit_support` - Dignity kit support details
- `head_gender_sr` / `head_gender_uf` - Household head gender
- `receiving_aid` - Receiving aid status
- `household_head` - Household head (coalesced)
- `hh_engaged_in_work_sr` / `hh_engaged_in_work_uf` - HH work engagement
- `forego_basic_essentials_sr` / `forego_basic_essentials_uf` - Forgoing essentials
- `number_of_meals_reduced_sr` / `number_of_meals_reduced_uf` - Reduced meals

#### Business Mentorship Fields (all end with _bm)
- `name_of_mentor_bm` - Mentor name
- `contact_of_mentor_bm` - Mentor contact
- `type_of_business_bm` - Business type
- `mentorship_county_bm` - Mentorship county
- `challenges_mentee_is_facing_bm` - Challenges
- `solutions_agreed_to_be_implemented_bm` - Solutions
- And many more _bm fields...

#### Empty Columns (commented out with -- EMPTY IN SOURCE)
17 columns were found to be completely empty and are commented out:
- `alternative_phone_number_csf`
- `county_apprenticeship_training_uf`
- `county_of_apprenticeship_apr`
- `county_of_apprenticeship_provider_apr`
- `did_participant_complete_the_traininng_placement_opportunity_tc`
- `Does_the_mentee_calculate_business_costs_before_setting_prices_bm`
- `during_your_interactions_at_the_tvet_did_you_experience_any_form_of_discrimination_abuse_harrassment_tvbl`
- `external_id`
- `if_no_indicate_the_reasons_apa`
- `if_yes_indicate_the_reasons_apa`
- `is_the_institute_recommended_for_partnership_apa`
- `MPESA_full-name_sp`
- `on_rate_does_applicantgroup_have_ability_to_leverage_finances_bga`
- `solutions_agreed_to_be_implemented_to_address_challenges_bm`
- `sub-county_dc`
- `sub-county_located_apbl`
- `whether_the_mentee_calculates_business_cost_before_setting_prices_bm`

### Coalesced Fields Pattern
Many fields use COALESCE to combine data from different program forms:
```sql
COALESCE(data -> 'properties' ->> 'field_sr', data -> 'properties' ->> 'field_uf') as field_name
```

### Field Suffixes
- `_sr` - Service Registration
- `_uf` - Upskilling Form  
- `_bm` - Business Mentorship
- `_pl` - Placement
- `_csf` - Coworking Space First visit
- `_csr` - Coworking Space Return visit
- `_dir` - Direct registration
- `_dka` - Dignity Kit Assessment
- `_dc` - Daycare
- `_bga` - Business Grant Assessment
- `_bg` - Business Grant
- And many more...

### Source Table
All data comes from: `{{ source('staging_sl', 'zzz_case') }}`

## Quick Reference for Common Tasks

### To find daycare-related fields:
```bash
grep -n "daycare\|children" staging_sl_case_table.sql
```

### To find dignity kit fields:
```bash
grep -n "dignity\|hygiene\|experienced_days\|miss_school" staging_sl_case_table.sql
```

### To find business mentorship fields:
```bash
grep -n "_bm\b" staging_sl_case_table.sql
```