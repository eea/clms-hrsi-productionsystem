#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os
import xml.etree.ElementTree as ET

default_metadata_version = '1.17'

def remove_rse_files(maja_product, verbose=0):
    """
    Remove SRE files from maja product
    :param maja_product: maja product directory
    :return:
    """
    if verbose > 0:
        print("Remove RSE files :")
    for file_name in os.listdir(maja_product):
        if "_SRE_" in file_name:
            if verbose > 0:
                print(file_name)
            file_path = os.path.join(maja_product, file_name)
            os.remove(file_path)


def remove_rse_references(metadata_file, verbose=0):
    """
    Remove SRE references in metadata file
    :param metadata_file: Maja product metadata xml file
    :return:
    """
    if verbose > 0:
        print("Remove RSE references into metadata file : {}".format(metadata_file))
    tree = ET.parse(metadata_file)
    root = tree.getroot()
    parents = root.findall(".//Image/..")
    for parent in parents:
        for img_node in parent.findall(".//Image"):
            if img_node[0][0].text == "Surface_Reflectance":
                rse_node = img_node
                parent.remove(rse_node)
    tree.write(metadata_file)


def remove_r2_files(maja_product, verbose=0):
    """
        Remove R2 files from maja product
        :param maja_product: maja product directory
        :return:
        """
    if verbose > 0:
        print("Remove R2 files :")
    for file_name in os.listdir(maja_product):
        if "_R2" in file_name and "SAT_R2" not in file_name and "DFP_R2" not in file_name and "DTF_R2" not in file_name:
            file_path = os.path.join(maja_product, file_name)
            if verbose > 0:
                print(file_path)
            os.remove(file_path)
    mask_folder = os.path.join(maja_product, "MASKS")
    for file_name in os.listdir(mask_folder):
        if "_R2" in file_name and "SAT_R2" not in file_name and "DFP_R2" not in file_name and "DTF_R2" not in file_name:
            file_path = os.path.join(mask_folder, file_name)
            if verbose > 0:
                print(file_path)
            os.remove(file_path)


def remove_r2_references(metadata_file, verbose=0):
    """
    Remove R2 references in metadata file
    :param metadata_file: Maja product metadata xml file
    :return:
    """
    if verbose > 0:
        print("Remove R2 references into metadata file : {}".format(metadata_file))
    tree = ET.parse(metadata_file)
    root = tree.getroot()
    parents = root.findall(".//*[@group_id='R2']/..")
    for parent in parents:
        for img_node in parent.findall(".//*[@group_id='R2']"):
            if img_node.tag == "MASK_FILE" or img_node.tag == "IMAGE_FILE":
                rse_node = img_node
                parent.remove(rse_node)
    tree.write(metadata_file)


def retrieve_metadata_file(maja_product, verbose=0):
    """
    Retrieve metadata_file in maja product
    :param maja_product: maja product directory
    :return: path to metadata file
    """
    for file_name in os.listdir(maja_product):
        if "_MTD_ALL" in file_name:
            metadata_file = os.path.join(maja_product, file_name)
    return metadata_file


def update_metadata_version(metadata_file, metadata_version=None, verbose=0):
    """
    Update metadata version number
    :param metadata_file: path to metadata_file
    :param metadata_version: new metadata version
    :return:
    """
    if metadata_version is None:
        metadata_version = default_metadata_version
    if verbose > 0:
        print("Update metadata version in metadata file : {}".format(metadata_file))
    tree = ET.parse(metadata_file)
    root = tree.getroot()
    node = root.findall(".//METADATA_FORMAT")[0]
    if node is not None:
        node.set("version", metadata_version)
        tree.write(metadata_file)
        


def maja_slim(maja_product, metadata_version=None, remove_r2=False, verbose=0):

    if metadata_version is None:
        metadata_version = default_metadata_version
    
    metadata_file = retrieve_metadata_file(maja_product, verbose=verbose)

    # remove RSE files
    remove_rse_files(maja_product, verbose=verbose)
    remove_rse_references(metadata_file, verbose=verbose)

    # Remove R2 files
    if remove_r2:
        remove_r2_files(maja_product, verbose=verbose)
        remove_r2_references(metadata_file, verbose=verbose)

    # Update metadata version
    if metadata_version is None:
        update_metadata_version(metadata_file, verbose=verbose)
    else:
        update_metadata_version(metadata_file, metadata_version, verbose=verbose)


if __name__ == "__main__":

    import argparse
    parser = argparse.ArgumentParser(description='This script is used to remove SRE bands from a MAJA L2A product which reduces it size by a factor of 2. It also removes SRE bands from metadata.')
    parser.add_argument("maja_product", type=str, help='path to MAJA L2A product')
    parser.add_argument("--metadata_version", type=str, default=default_metadata_version, help='metadata version. default is %s.'%default_metadata_version)
    parser.add_argument("--remove_r2", action='store_true', help='remove R2 files i.e. remove 20m resolution files where an original 10m resolution exists.')
    parser.add_argument("--verbose", type=int, default=1, help='verbose level: 0=silent, 1=chatty. default is 1')
    args = parser.parse_args()

    maja_slim(args.maja_product, metadata_version=args.metadata_version, remove_r2=args.remove_r2, verbose=args.verbose)




