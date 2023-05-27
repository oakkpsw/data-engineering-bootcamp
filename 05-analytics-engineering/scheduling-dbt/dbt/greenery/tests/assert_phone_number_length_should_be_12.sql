select length(phone_number)

from {{ ref('stg_greenery__users') }}
where length(phone_number) != 12
