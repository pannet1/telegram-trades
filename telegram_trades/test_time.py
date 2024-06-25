from toolkit.kokoo import blink
import pendulum

"""
add minutes to pendulum time and check if it is already past
"""
minutes = 1
ts = "13/06/2024 05:10:00"
ts = pendulum.parse(ts.split(" ")[1]).add(minutes=minutes).set(tz="Asia/Kolkata")

if pendulum.now() >= ts:
    ts = ts.to_datetime_string()
    print(ts)
else:
    print(f"{pendulum.now()}>{ts.to_datetime_string}")
    blink()
