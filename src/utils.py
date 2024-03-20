import numpy as np
import rasterio as rio
import os
from rasterio.enums import Resampling

def clean(reference, to_clean):
    """Cleans 2 numpy arrays of same dimensions, where the reference == 0 then make the corresponding cell in to_clean = 0
    Args:
        reference: numpy array
        to_clean: numpy array
    Returns:
        numpy array
    """
    cleaned_img = to_clean.squeeze().copy()
    cleaned_img[reference == 0] = 0
    return cleaned_img

def resample(reference, to_resize):
    """Resamples a rasterio image (to_resize) to fit the reference image (reference)
    Args:
        reference: path to a tif, the one that will be used as reference
        to_resize: path to a tif, the one that will be resampled to fit reference
    Returns: 
        (numpy.ndarray, affine.Affine)
    """
    import rasterio as rio
    
    high_res_img = rio.open(reference).read(1).squeeze()
    low_res_img = rio.open(to_resize).read(1).squeeze()
    high_res = high_res_img.shape
    low_res = low_res_img.shape
    
    print(f"high_res {high_res} -> low_res{low_res}")
    width_upscale = high_res[0]/low_res[0]
    height_upscale = high_res[1]/low_res[1]
    
    with rio.open(to_resize) as dataset:
        data = dataset.read(
            out_shape=(
                dataset.count,
                int(dataset.height * height_upscale),
                int(dataset.width * width_upscale)
            ),
            resampling = Resampling.bilinear # Maybe other resampling methods work better
        ).squeeze()
    
        # scale image transform
        transform = dataset.transform * dataset.transform.scale(
            (dataset.width / data.shape[-1]),
            (dataset.height / data.shape[-2])
        )
    return data, transform