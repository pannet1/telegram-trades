target_range = "0.05 | 0.06  | 0.07"
target_range = target_range.split("|")
target_range = list(map(float, target_range))
ltp = 0.10

# find value above the ltp in the target target_range
# if found return the value
for k, v in enumerate(target_range):
    if ltp < v:
        idx = k - 1
        if idx >= 0:
            print(target_range[idx])
            print(v - 0.05)


if ltp > float(target_range[-1]):
    print(target_range[-1])
