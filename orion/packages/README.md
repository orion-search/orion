# Packages #
A collection of modules used in Orion.

## Microsoft Academic Knowledge API ##

### Getting an API key ###
* Sign up for an API Management account with [Microsoft Research](https://msr-apis.portal.azure-api.net/signup).
* To activate your account, log into the email you used during the registration, open the _Please confirm your new Microsoft Research APIs account_ email and click on the activation link.
* Click on the **Subscribe** button and choose **Project Academic Knowledge**.
* Click again on the **Subscribe** button and then **Confirm** your choice.
* You can now use the **Primary key** to query the API.

### Using your API key in this project ###
The Microsoft Academic API key is stored in the `orion/orion/core/config/orion_config.config` file with the following format:

```
[mag]
MAG_API_KEY=MY_MAG_API_KEY
```

<!-- The  Microsoft Academic API key is stored in the `.env` file with the following format: -->

<!-- ``` -->
<!-- mag_key = MY_API_KEY -->
<!-- ``` -->

To learn how to use the API, check the [official documentation](https://docs.microsoft.com/en-us/azure/cognitive-services/academic-knowledge/home).

## Google Places API ##

### Getting an API key ###
* Sign in with your Google account to [Google Cloud Platform (GCP)](https://console.cloud.google.com/). 
* Set up a project and enable billing.
* Find the **Places API** in the **Marketplace** and enable it.
* Click on the **CREDENTIALS** tab and generate an API key.

### Using your API key in this project ###
The Google API key is stored in the  `orion/orion/core/config/orion_config.config` file with the following format:

```
[google]
GOOGLE_KEY=MY_GOOGLE_API_KEY
```
<!-- The Google API key is stored in the `.env` file with the following format: -->

<!-- ``` -->
<!-- mag_key = MY_API_KEY -->
<!-- ``` -->

To learn how to use the API, check the [official documentation](https://developers.google.com/places/web-service/details).

## Gender API ##

### Getting an API key ###
Sign up on [Gender API](https://gender-api.com/en/) in order to get an API key.

### Using your API key in this project ###
The Gender API key is stored in the `orion/orion/core/config/orion_config.config` file with the following format:

```
[genderapi]
GENDER_API_KEY=MY_GENDER_API_KEY
```

**Note**: All of the above are paid services.
# Packages #
Orion module.
