#field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split, field_filter
table_cn = '{{table_cn}}'
field_array = [
{% for field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split, field_filter in field_array %}
    ('{{field_en}}', '{{field_cn}}', '{{field_to_ctbase}}', '{{field_filter}}'),
{% %}
]
