from basic import cursor, get, count, multicount

# family_number = count('famecon_2010')
# agri_number = count('famecon_2010', 'fk1=1')

# rural_number = count('famecon_2010', 'urban=0')
# agri_rural_number = count('famecon_2010', 'urban=0 and fk1=1')

# agri_ratio = agri_number / family_number
# rural_agri_ratio = agri_rural_number / rural_number

family_numbers = multicount('famecon')

rural_numbers = multicount(
    'famecon',
    conditions="urban{year}=0",
    condition_format=['', '12', '14', '16', '18']
    )

rural_agri_numbers = multicount(
    'famecon',
    conditions=[
        'urban=0 and fk1=1',
        'urban12=0 and fk1l=1',
        'urban14=0 and fk1l=1',
        'urban16=0 and fk1l=1',
        'urban18=0 and fk1l=1'
    ]
)

rural_agri_ratios = [b/a for a, b in zip(rural_numbers, rural_agri_numbers)]

print(rural_agri_ratios)
print('qwq')