import re


def clean_name(name):
    """Find the full fornames and assign a NaN value to the rest. 
        Args: 
            name (:obj:`str`): Forename of a person. 
   
        Returns: 
            (:obj:`str`) or (:obj:`np.nan`), depending on the string. 
    
    """
    first_name = " ".join(name.split(" ")[:-1])
    last_name = name.split(" ")[-1]

    # Remove initials
    first_name = re.sub("(.?)\.", "", first_name).strip()
    if len(first_name) > 1:
        return " ".join([first_name, last_name])
    else:
        return None


def identity_tokenizer(tokens):
    """Passes tokens without processing. Used in a CountVectorizer.

    Args:
        tokens (:obj:`list`)
    
    Returns:
        tokens (:obj:`list`)

    """
    return tokens
