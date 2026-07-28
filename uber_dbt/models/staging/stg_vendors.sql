-- Static lookup table for VendorID codes
-- Source: NYC TLC data dictionary

with vendors as (

    select * from (values
        ('1', 'Creative Mobile Technologies (CMT)'),
        ('2', 'VeriFone Inc.')
    ) as t(vendor_id, vendor_name)

)

select * from vendors