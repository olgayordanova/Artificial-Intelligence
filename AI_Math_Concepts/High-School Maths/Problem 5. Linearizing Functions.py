import math
import numpy as np
import matplotlib.pyplot as plt

x_plot = np.linspace(-1,1,100)
y_plot = math.log(3, math.e)+3*x_plot

plt.plot(x_plot, y_plot)
ax = plt.gca()
ax.spines["bottom"].set_position("zero")
ax.spines["left"].set_position("zero")
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
plt.show()
