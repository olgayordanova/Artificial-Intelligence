# # Draw a Koch snowflake
from turtle import *

def koch(a, order):
    if order > 0:
        for t in [60, -120, 60, 0]:
            koch(a/3, order-1)
            left(t)
    else:
        forward(a)


# Choose colours and size
color("red", "white")
pensize(3)
size = 400
order = 3

# Ensure snowflake is centred
penup()
backward(size/1.732)
left(30)
pendown()

# Make it fast
tracer(100)
hideturtle()

begin_fill()
# Four Koch curves
for i in range(4):
    koch(size, order)
    right(120)
end_fill()

done()
