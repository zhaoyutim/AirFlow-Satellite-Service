import argparse
import logging
import os
import numpy as np
import torch
import torch.nn.functional as F
import glob
import cv2
from torchvision import transforms
import torch
import torch.nn.functional as F
import logging
import numpy as np
import torch
from S2Unet import UNet

def predict_img(net, full_img, device, transform, out_threshold=0.5):
    net.eval()
    img = torch.from_numpy(full_img)
    
    img = transform(img).unsqueeze(0)
    img = img.to(device=device, dtype=torch.float32)

    with torch.no_grad():
        output = net(img).cpu()
        if net.n_classes > 1:
            mask = torch.sigmoid(output[:, 1]) > out_threshold
        else:
            mask = torch.sigmoid(output) > out_threshold

    return mask[0].long().squeeze().numpy()

def get_args():
    parser = argparse.ArgumentParser(description='Predict masks from input images')
    parser.add_argument('--model', '-m', default='MODEL.pth', metavar='FILE',
                        help='Specify the file in which the model is stored')
    parser.add_argument('--input', '-i', metavar='INPUT', help='Directory of input images', required=True)
    parser.add_argument('--output', '-o', default="outputs/", metavar='OUTPUT', help='Directory of output images')    
    return parser.parse_args()

class NormalizeCV2(object):
    def __call__(self, image):
        image = np.nan_to_num(image, nan=0.0)
        return cv2.normalize(image, None, -1, 1, cv2.NORM_MINMAX)

if __name__ == '__main__':
    args = get_args()
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    input_dir = args.input

    # Get sorted list of files
    predict_files = sorted(glob.glob(f"{input_dir}/*.npy"))
    print("Found",len(predict_files), "files.")
    # Calculate the number of validation files based on the percentage
    
    # Use the last n_val files for prediction (mimicking the validation set)
    output_dir = args.output
    os.makedirs(output_dir, exist_ok=True)

    net = UNet(n_channels=3, n_classes=1, bilinear=False)

    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    logging.info(f'Loading model {args.model}')
    logging.info(f'Using device {device}')

    net.to(device=device)
    state_dict = torch.load(args.model, map_location=device)
    mask_values = state_dict.pop('mask_values', [0, 255])
    net.load_state_dict(state_dict)
    logging.info('Model loaded!')

    transform = transforms.Compose([
        NormalizeCV2(),
        transforms.ToTensor()
    ])

    for i, fn in enumerate(predict_files):
        logging.info(f'\nPredicting image {fn} ...')
        img = np.load(fn)

        mask = predict_img(net=net,
                           full_img=img,
                           transform=transform,
                           out_threshold=0.5,
                           device=device)

        out_filename = os.path.join(output_dir, os.path.basename(fn))
        np.save(out_filename, mask)
        logging.info(f'Mask saved to {out_filename}')