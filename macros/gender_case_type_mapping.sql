{% macro gender_case_category(assault_type='assault_type', sexual_assault_type='sexual_assault_type', other_assault_type='other_assault_type') %}
    {% set assault = "coalesce(lower(" ~ assault_type ~ "), '')" %}
    {% set sexual = "coalesce(lower(" ~ sexual_assault_type ~ "), '')" %}
    {% set other = "coalesce(lower(" ~ other_assault_type ~ "), '')" %}
    {% set combined = assault ~ " || ' ' || " ~ sexual ~ " || ' ' || " ~ other %}

    case
        when {{ combined }} ~ 'technologically_facilitated_violence|cyber|stalking|ocsea|online sexual|child pornography|pornography|digital'
            then 'Technologically facilitated violence'
        when {{ combined }} ~ 'harmful_traditional_practice|harmful traditional|harmful cultural|fgm|female circum|circumsc|gum cutting|early marriage|forced marriage|child marriage|traditional practice'
            then 'Harmful Traditional practice'
        when {{ combined }} ~ 'child_abuse|child abuse|child neglect|child negligence|juvenile_cases|juvenile|child labour|child labor|child trafficking|adult trafficking|forced prostitution|child exploitation|child prostitution|child delinquen|delinquen|truancy|neglect'
            then 'Child abuse'
        when {{ combined }} ~ 'sexual_violence|rape|defilement|sodom|indecent|incest|sexual har|gang_rape|gang rape|sexual_assault|attempted_rape|attempted rape|attempted_defilement|attempted defilement|molestation|sexual abuse|sexual exploitation'
            then 'Sexual violence'
        when {{ combined }} ~ 'physical_violence|domestic_violence|domestic violence|physical assault|physical abuse|femicide|robbery with violence|grievous harm|grevious harm|unlawful confinement|torture|assault'
            then 'Physical violence'
        when {{ combined }} ~ 'psychological_or_emotional_abuse|psychological|emotional|verbal|threat|harass|intimidat|isolation|mental abuse|psychosocial abuse'
            then 'Psychological or emotional abuse'
        when nullif(trim(coalesce({{ sexual_assault_type }}, '')), '') is not null
            then 'Sexual violence'
        else 'Other'
    end
{% endmacro %}


{% macro gender_case_sub_type(assault_type='assault_type', sexual_assault_type='sexual_assault_type', other_assault_type='other_assault_type') %}
    case
        when nullif(trim(coalesce({{ sexual_assault_type }}, '')), '') is not null
            then initcap(replace(trim({{ sexual_assault_type }}), '_', ' '))
        when
            nullif(trim(coalesce({{ other_assault_type }}, '')), '') is not null
            and lower(trim({{ other_assault_type }})) not in ('nan', 'n/a', 'na', '1', '2', 'choice2')
            then initcap(regexp_replace(replace(trim({{ other_assault_type }}), '_', ' '), '\s+', ' ', 'g'))
        when
            nullif(trim(coalesce({{ assault_type }}, '')), '') is not null
            and lower(trim({{ assault_type }})) not in (
                'physical_violence',
                'sexual_violence',
                'psychological_or_emotional_abuse',
                'harmful_traditional_practice',
                'child_abuse',
                'technologically_facilitated_violence'
            )
            then initcap(regexp_replace(replace(trim({{ assault_type }}), '_', ' '), '\s+', ' ', 'g'))
        else null
    end
{% endmacro %}
