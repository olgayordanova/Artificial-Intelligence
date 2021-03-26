import numpy as np
import matplotlib.pyplot as plt
from celluloid import Camera
from IPython.display import HTML

fig = plt.figure(figsize=(4, 4), facecolor="w")
camera = Camera(fig)

def calculate_derivative_at_point(function, point, precision=1e-7):
    return (function ( point + precision ) - function ( point )) / precision

def plot_derivative_at_point_animation(function, point, derivative=None, min_x=-10, max_x=10):
    vectorized_function = np.vectorize ( function )

    x = np.linspace ( min_x, max_x, 1000 )
    y = vectorized_function ( x )

    slope = 0  # Slope of the tangent line
    if derivative is None:
        slope = calculate_derivative_at_point ( function, point )
    else:
        slope = derivative ( point )

    intercept = function ( point ) - slope * point
    tangent_line_x = np.linspace ( point - 2, point + 2, 10 )
    tangent_line_y = slope * tangent_line_x + intercept
    plt.plot ( x, y )
    plt.plot ( tangent_line_x, tangent_line_y )
    plt.grid ( True )
    camera.snap ()

#
# for x in np.arange(-8, 10, 0.1):
#     plot_derivative_at_point_animation(lambda x: x ** 2, x)


# for x in np.arange(-8, 10, 0.2):
#     plot_derivative_at_point_animation(np.sin, x)

for x in np.arange(-8, 10, 0.2):
    plot_derivative_at_point_animation(lambda x: 0.5 ** x, x)

anim = camera.animate ()
anim.save ( "test2.gif" )
HTML ( anim.to_html5_video () )
