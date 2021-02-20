import numpy as np
import matplotlib.pyplot as plt

def get_x_plot(min_x, max_x, num_points):
    x_plot = np.linspace(min_x,max_x,num_points)
    return x_plot


def get_y_plot(x,f):
    f_vectorized = np.vectorize(f)
    y_plot = f_vectorized(x)
    return y_plot

def draw_plot(x_plot, y_plot):
    plt.plot(x_plot, y_plot)
    ax = plt.gca()
    ax.spines["bottom"].set_position("zero")
    ax.spines["left"].set_position("zero")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.axis('equal')
    plt.show()

def plot_math_function(f, min_x, max_x, num_points):
    x_plot = get_x_plot(min_x, max_x, num_points)
    y_plot = get_y_plot(x_plot,f)
    draw_plot ( x_plot, y_plot )

plot_math_function(lambda x: 2 * x + 3, -3, 5, 1000)
plot_math_function(lambda x: -x + 8, -1, 10, 1000)
plot_math_function(lambda x: x**2 - x - 2, -3, 4, 1000)
plot_math_function(lambda x: np.sin(x), -np.pi, np.pi, 1000)
plot_math_function(lambda x: np.sin(x) / x, -4 * np.pi, 4 * np.pi, 1000)