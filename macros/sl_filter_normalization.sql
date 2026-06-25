{% macro normalize_sl_yes_no_filter(value_expr) -%}
{%- set normalized_value = "regexp_replace(lower(trim(coalesce(" ~ value_expr ~ "::text, ''))), '[^a-z0-9]+', '', 'g')" -%}
case
    when {{ value_expr }} is null or trim({{ value_expr }}::text) = '' then null
    when {{ normalized_value }} in ('yes', 'y', 'true', '1') then 'Yes'
    when {{ normalized_value }} in ('no', 'n', 'false', '0') then 'No'
    else null
end
{%- endmacro %}

{% macro normalize_sl_nationality_filter(value_expr, refugee_type_expr='null', por_expr='null') -%}
{%- set cleaned_value = "lower(trim(coalesce(" ~ value_expr ~ "::text, '')))" -%}
{%- set cleaned_refugee_type = "lower(trim(coalesce(" ~ refugee_type_expr ~ "::text, '')))" -%}
{%- set cleaned_por = "lower(trim(coalesce(" ~ por_expr ~ "::text, '')))" -%}
{%- set value_slug = "regexp_replace(" ~ cleaned_value ~ ", '[^a-z0-9]+', '', 'g')" -%}
{%- set refugee_type_slug = "regexp_replace(" ~ cleaned_refugee_type ~ ", '[^a-z0-9]+', '', 'g')" -%}
{%- set por_slug = "regexp_replace(" ~ cleaned_por ~ ", '[^a-z0-9]+', '', 'g')" -%}
case
    when ({{ value_expr }} is null or trim({{ value_expr }}::text) = '')
        and ({{ refugee_type_expr }} is null or trim({{ refugee_type_expr }}::text) = '')
        and ({{ por_expr }} is null or trim({{ por_expr }}::text) = '')
        then null
    when {{ value_slug }} in (
        '', 'none', 'null', 'na', 'n', 'no', 'unknown', 'notapplicable',
        'f', 'm', 'yes'
    )
        or {{ cleaned_value }} ~ '^[0-9]{4,}$'
        then null
    when {{ refugee_type_slug }} not in ('', 'host', 'kenyan', 'none', 'null', 'na', 'n', 'unknown')
        then 'Refugees'
    when {{ por_slug }} not in ('', 'host', 'kenyan', 'none', 'null', 'na', 'n', 'unknown')
        then 'Refugees'
    when {{ value_slug }} in ('kenyan', 'kenya', 'host') then 'Kenyan'
    when {{ value_slug }} in ('refugee', 'refugees') then 'Refugees'
    when {{ value_slug }} in ('nonkenyan', 'nonkenyans', 'noncitizenanonkenyanthatisnotarefugee') then 'Non-Kenyans'
    when {{ value_expr }} is not null and trim({{ value_expr }}::text) <> '' then 'Non-Kenyans'
    else null
end
{%- endmacro %}

{% macro normalize_sl_county_filter(value_expr) -%}
{%- set cleaned_value = "trim(regexp_replace(replace(replace(lower(coalesce(" ~ value_expr ~ "::text, '')), '_', ' '), '-', ' '), '\\s+', ' ', 'g'))" -%}
{%- set cleaned_without_suffix = "trim(regexp_replace(" ~ cleaned_value ~ ", ' county$', '', 'g'))" -%}
{%- set value_slug = "regexp_replace(" ~ cleaned_without_suffix ~ ", '[^a-z0-9]+', '', 'g')" -%}
case
    when {{ value_expr }} is null or trim({{ value_expr }}::text) = '' then null
    when {{ cleaned_without_suffix }} ~ '^[0-9]{1,}$' then null
    when {{ cleaned_without_suffix }} ~ '^[0-9]{7,}$' then null
    when {{ value_slug }} in ('', 'none', 'null', 'na', 'n', 'unknown', 'business', 'busis') then null
    when {{ value_slug }} in (
        'nairobi', 'nairobicounty', 'nbi', 'dagorettisouth', 'dagorettinorth', 'embakasieast',
        'embakasinorth', 'embakasicentral', 'embakasiwest', 'langata', 'kibra', 'mathare',
        'makadara', 'kamukunji', 'starehe', 'westlands', 'ruaraka', 'roysambu', 'kasarani',
        'njiru'
    ) then 'Nairobi'
    when {{ value_slug }} in ('mombasa', 'mombasacounty', 'monbasa', 'changamwe', 'jomvu', 'likoni', 'nyali', 'kisauni', 'kiauni') then 'Mombasa'
    when {{ value_slug }} in ('kilifi', 'kilificounty', 'kiii', 'kilif') then 'Kilifi'
    when {{ value_slug }} in ('kisumu', 'kiumu') then 'Kisumu'
    when {{ value_slug }} in ('siaya', 'iaya', 'sya', 'bondo') then 'Siaya'
    when {{ value_slug }} in ('homabay', 'homaboy', 'hby') then 'Homa Bay'
    when {{ value_slug }} in ('machakos', 'machako', 'machakosi', 'mck') then 'Machakos'
    when {{ value_slug }} in ('migori', 'mrg') then 'Migori'
    when {{ value_slug }} in ('bungoma', 'bngoma', 'bugoma', 'bungom', 'bungomaa', 'bgm') then 'Bungoma'
    when {{ value_slug }} in ('kakamega', 'kakamenga', 'kkg') then 'Kakamega'
    when {{ value_slug }} in ('busia', 'buia', 'bsa') then 'Busia'
    when {{ value_slug }} in ('nakuru', 'nkr') then 'Nakuru'
    when {{ value_slug }} in ('nyeri', 'nyr') then 'Nyeri'
    when {{ value_slug }} in ('makueni', 'mkn') then 'Makueni'
    when {{ value_slug }} in ('kajiado') then 'Kajiado'
    when {{ value_slug }} in ('kirinyaga', 'kirinyanga') then 'Kirinyaga'
    when {{ value_slug }} in ('kwale') then 'Kwale'
    when {{ value_slug }} in ('meru') then 'Meru'
    when {{ value_slug }} in ('nyamira') then 'Nyamira'
    when {{ value_slug }} in ('kitui') then 'Kitui'
    when {{ value_slug }} in ('kisii', 'kisi', 'kissi') then 'Kisii'
    when {{ value_slug }} in ('taitataveta', 'taita', 'tvt') then 'Taita Taveta'
    when {{ value_slug }} in ('uasingishu', 'uasingichu', 'uasngishu', 'uaingihu', 'ausingishu', 'ausnigishu', 'eldoret') then 'Uasin Gishu'
    when {{ value_slug }} in ('transnzoia', 'transzoia', 'trannzoia', 'tranzoia', 'kitale', 'cheregani') then 'Trans Nzoia'
    when {{ value_slug }} in ('elgeyomarakwet') then 'Elgeyo Marakwet'
    when {{ value_slug }} in ('tanariver') then 'Tana River'
    when {{ value_slug }} in ('vihiga', 'vhg') then 'Vihiga'
    when {{ value_slug }} in ('muranga') then 'Muranga'
    else initcap({{ cleaned_without_suffix }})
end
{%- endmacro %}

{% macro normalize_sl_subcounty_filter(value_expr) -%}
{%- set cleaned_value = "trim(regexp_replace(replace(replace(lower(coalesce(" ~ value_expr ~ "::text, '')), '_', ' '), '-', ' '), '\\s+', ' ', 'g'))" -%}
{%- set cleaned_without_suffix = "trim(regexp_replace(regexp_replace(" ~ cleaned_value ~ ", ' subcounty$', '', 'g'), ' sub county$', '', 'g'))" -%}
{%- set value_slug = "regexp_replace(" ~ cleaned_without_suffix ~ ", '[^a-z0-9]+', '', 'g')" -%}
case
    when {{ value_expr }} is null or trim({{ value_expr }}::text) = '' then null
    when {{ cleaned_without_suffix }} = '' then null
    when {{ cleaned_without_suffix }} ~ '^[0-9]{1,}$' then null
    when {{ cleaned_without_suffix }} ~ '^[0-9]{7,}$' then null
    when {{ value_slug }} in ('none', 'null', 'na', 'n', 'unknown', 'subcounty') then null
    when {{ value_slug }} in ('kibra', 'kibera') then 'Kibra'
    when {{ value_slug }} in ('mathare') then 'Mathare'
    when {{ value_slug }} in ('changamwe') then 'Changamwe'
    when {{ value_slug }} in ('jomvu') then 'Jomvu'
    when {{ value_slug }} in ('likoni') then 'Likoni'
    when {{ value_slug }} in ('kisauni', 'kiauni') then 'Kisauni'
    when {{ value_slug }} in ('embakaisouth', 'embakaiouth', 'embakasisouth', 'embakazisouth', 'embkasisouth') then 'Embakasi South'
    when {{ value_slug }} in ('embakainorth', 'embakasinorth') then 'Embakasi North'
    when {{ value_slug }} in ('embakaieast', 'embakaieat', 'embakasieast', 'eastembakasi') then 'Embakasi East'
    when {{ value_slug }} in ('embakaicentral', 'embakasicentral') then 'Embakasi Central'
    when {{ value_slug }} in ('dagorettinorth', 'dagorettinotth', 'dagoretinorth', 'dagorettin', 'dagoretin') then 'Dagoretti North'
    when {{ value_slug }} in ('dagorettisouth', 'dagoretisouth', 'dagorethisouth') then 'Dagoretti South'
    when {{ value_slug }} in ('royambu', 'roysambu') then 'Roysambu'
    when {{ value_slug }} in ('wetland', 'westlands') then 'Westlands'
    when {{ value_slug }} in ('nakurutownwet', 'nakuruwest') then 'Nakuru Town West'
    when {{ value_slug }} in ('nakurueast') then 'Nakuru Town East'
    when {{ value_slug }} in ('webuyeeat') then 'Webuye East'
    when {{ value_slug }} in ('kiumucentral') then 'Kisumu Central'
    when {{ value_slug }} in ('kiumueat', 'kiumueast') then 'Kisumu East'
    when {{ value_slug }} in ('kiumuwet', 'kiumuwest') then 'Kisumu West'
    when {{ value_slug }} in ('nyeritown', 'nyericentral', 'nyeritownship') then 'Nyeri Town'
    when {{ value_slug }} in ('homabaytown') then 'Homa Bay Town'
    when {{ value_slug }} in ('alego', 'centralalego', 'northalego', 'westalego', 'eastalego', 'southeastalego', 'alegouonga', 'alegousonga', 'alegousoga', 'alegoosonga', 'alegousenga', 'alegousongo', 'alegousongs') then 'Alego Usonga'
    when {{ value_slug }} in ('mtelgon', 'mountelgon') then 'Mount Elgon'
    else initcap({{ cleaned_without_suffix }})
end
{%- endmacro %}
