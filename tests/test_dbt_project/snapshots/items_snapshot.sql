{% snapshot items_snapshot %}
    {{
        config(
          unique_key='item_id',
          strategy='timestamp',
          updated_at='updated_at'
        )
    }}
    select * from {{ ref('items') }}
{% endsnapshot %}