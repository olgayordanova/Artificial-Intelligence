import cv2
import numpy as np

input_img_file = "test.png"

# Degree conversion
def DegreeTrans(theta):
    res = theta / np.pi * 180
    return res


# Rotate the image degree counterclockwise (original size)
def rotateImage(src, degree):
    # The center of rotation is the center of the image
    h, w = src.shape[:2]
    # Calculate the two-dimensional rotating affine transformation matrix
    RotateMatrix = cv2.getRotationMatrix2D ( (w / 2.0, h / 2.0), degree, 1 )
    print ( RotateMatrix )
    # Affine transformation, the background color is filled with white
    rotate = cv2.warpAffine ( src, RotateMatrix, (w, h), borderValue=(255, 255, 255) )
    return rotate


# Calculate angle by Hough transform
def CalcDegree(srcImage):
    midImage = cv2.cvtColor ( srcImage, cv2.COLOR_BGR2GRAY )
    dstImage = cv2.Canny ( midImage, 50, 200, 3 )
    lineimage = srcImage.copy ()

    # Detect straight lines by Hough transform
    # The fourth parameter is the threshold, the greater the threshold, the higher the detection accuracy
    lines = cv2.HoughLines ( dstImage, 1, np.pi / 180, 200 )
    # Due to different images, the threshold is not easy to set, because the threshold is set too high, so that the line cannot be detected, the threshold is too low, the line is too much, the speed is very slow
    sum = 0
    # Draw each line segment in turn
    for i in range ( len ( lines ) ):
        for rho, theta in lines[i]:
            # print("theta:", theta, " rho:", rho)
            a = np.cos ( theta )
            b = np.sin ( theta )
            x0 = a * rho
            y0 = b * rho
            x1 = int ( round ( x0 + 1000 * (-b) ) )
            y1 = int ( round ( y0 + 1000 * a ) )
            x2 = int ( round ( x0 - 1000 * (-b) ) )
            y2 = int ( round ( y0 - 1000 * a ) )
            # Only select the smallest angle as the rotation angle
            sum += theta
            cv2.line ( lineimage, (x1, y1), (x2, y2), (0, 0, 255), 1, cv2.LINE_AA )
            cv2.imshow ( "Imagelines", lineimage )

        # Averaging all angles, the rotation effect will be better
    average = sum / len ( lines )
    angle = DegreeTrans ( average ) - 90
    return angle


if __name__ == '__main__':
    image = cv2.imread ( input_img_file )
    cv2.imshow ( "Image", image )
    # Tilt angle correction
    degree = CalcDegree ( image )
    print ( "Adjust angle:", degree )
rotate = rotateImage ( image, degree )
cv2.imshow ( "rotate", rotate )
# cv2.imwrite("../test/recified.png", rotate, [int(cv2.IMWRITE_PNG_COMPRESSION), 0])
cv2.waitKey ( 0 )
cv2.destroyAllWindows ()