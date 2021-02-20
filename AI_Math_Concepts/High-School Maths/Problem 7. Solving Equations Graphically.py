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



plot_math_functions([lambda x: 2 * x + 3, lambda x: 0], -3, 5, 1000)
plot_math_functions([lambda x: 3 * x**2 - 2 * x + 5, lambda x: 3 * x + 7], -2, 3, 1000)
plot_math_functions([lambda x: (-4 * x + 7) / 3, lambda x: (-3 * x + 8) / 5, lambda x: (-x - 1) / -2], -1, 4, 1000)

# vectorized_fs = [np.vectorize(f) for f in functions]
# ys = [vectorized_f(x) for vectorized_f in vectorized_fs]