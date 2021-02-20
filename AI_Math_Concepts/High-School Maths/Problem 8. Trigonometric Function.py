import math
import numpy as np
import matplotlib.pyplot as plt

def get_x_plot(min_x, max_x, num_points):
    x_plot = np.linspace(min_x,max_x,num_points)
    return x_plot


def get_y_plot(x,f):
    f_vectorized = np.vectorize(f)
    y_plot = f_vectorized(x)
    return y_plot

def draw_plot(x_plot, y_plots):
    for y_plot in y_plots:
        # if y_plot !='nan':
        plt.plot(x_plot, y_plot)
    ax = plt.gca()
    ax.spines["bottom"].set_position("zero")
    ax.spines["left"].set_position("zero")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.axis('equal')
    plt.show()

def plot_math_functions(functions, min_x, max_x, num_points):
    x_plot = get_x_plot ( min_x, max_x, num_points )
    y_plots=[]

    for f in functions:
        y_plot = get_y_plot(x_plot,f)
        y_plots.append(y_plot)
    draw_plot ( x_plot, y_plots )


plot_math_functions([lambda x: np.arcsin(x)], -1, 1, 120)
plot_math_functions([lambda x: np.arccos(x)], -1, 1, 120)
plot_math_functions([lambda x:np.arctan(x), ], -np.pi, np.pi, 120)
plot_math_functions([lambda x:math.pi/2-np.arctan(x)], -np.pi, np.pi, 120)

 # just remove nan elements from vector

#  -np.pi, np.pi, 12
# [deg]=180/𝜋.[rad],[rad]=𝜋/180.[deg]
# This can be done using np.deg2rad() and np.rad2deg() respectively.
# x=np.rad2deg(1)
# print(x)
# y=np.deg2rad(90/math.pi)
# print(y)