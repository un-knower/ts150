#field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split, field_filter
table_cn = '{{table_cn}}'
field_array = [
{% for field_en, field_cn, field_type, field_length, field_is_pk, field_is_dk, field_to_ctbase, index_describe, index_function, index_split, field_filter in field_array %}
    {{"(%-28s %-30s %-11s '%s', %-4s %-4s %-4s '%s', '%s', '%s', %-3s)," % ("'%s'," % field_en, "'%s'," % field_cn, "'%s'," % field_type, field_length, "'%s'," % field_is_pk, "'%s'," % field_is_dk, "'%s'," % field_to_ctbase, index_describe, index_function, index_split, "'%s'" % field_filter)}}
{% %}
]
