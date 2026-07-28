-- Static lookup table for payment_type codes
-- Source: NYC TLC data dictionary

with payment_types as (

    select * from (values
        ('1', 'Credit Card'),
        ('2', 'Cash'),
        ('3', 'No Charge'),
        ('4', 'Dispute'),
        ('5', 'Unknown'),
        ('6', 'Voided Trip')
    ) as t(payment_type_id, payment_type_name)

)

select * from payment_types