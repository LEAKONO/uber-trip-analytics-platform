

with rate_codes as (

    select * from (values
        ('1', 'Standard Rate'),
        ('2', 'JFK Airport'),
        ('3', 'Newark Airport'),
        ('4', 'Nassau or Westchester'),
        ('5', 'Negotiated Fare'),
        ('6', 'Group Ride')
    ) as t(rate_code_id, rate_code_name)

)

select * from rate_codes