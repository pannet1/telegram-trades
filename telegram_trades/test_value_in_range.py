target_range = "0.5 | 0.6  | 0.7 | 0.9 |1"
target_range = target_range.split("|")
target_range = list(map(float, target_range))
ltp = 1
order = {"trigger_price": 0.7}
print(target_range)
# find value above the ltp in the target target_range
# if found return the value
for idx, v in enumerate(target_range):
    if idx > 0:
        intended_stop = target_range[idx - 1]
        trigger = float(order["trigger_price"])
        print(
            f"{ltp=}>={v=} {ltp>=v} and {intended_stop=}>{trigger=}{intended_stop > trigger}"
        )
        if ltp >= v and intended_stop > trigger:
            print(f"trailing from {trigger} to {intended_stop} success")
            break
