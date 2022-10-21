
set_a = {1, 2}
df = {1, 2, 3}
config = {1, 2, 4}

print(set_a.difference(df))
print(config.difference(set_a))
print(set_a.difference(config))
print(config.difference(df))

if df.difference(config):
   print('Not valid values')
   