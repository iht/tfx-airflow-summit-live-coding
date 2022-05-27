import tensorflow_transform as tft


def preprocessing_fn(input_tensors):
    columns = ['petal_length', 'petal_width', 'sepal_length', 'sepal_width']
    output_dict = {}
    for col in columns:
        output_dict[col] = tft.scale_to_0_1(input_tensors[col])

    output_dict['species'] = input_tensors['species']

    return output_dict
