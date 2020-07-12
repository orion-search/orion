# External Dependencies #

To use Orion, you need to create an account on Amazon Web Services (AWS), Google Cloud (Places API), Gender API and Microsoft Academic Knowledge API.

## **Obtaining API keys** ##

### **Microsoft Academic Knowledge API** ###

- Sign up for an account with [Microsoft Research](https://msr-apis.portal.azure-api.net/signup).
- Activate your account and subscribe to the **Project Academic Knowledge**.
- You can use the **Primary Key** in your profile page to query the API. Test queries using the *Evaluate* method in the [API Explorer](https://msr-apis.portal.azure-api.net/docs/services/academic-search-api/).

### **Google Places API** ###

- Sign in with your Google account to [Google Cloud Platform (GCP)](https://console.cloud.google.com/).
- Set up a project and enable billing.
- Find the **Places API** in the **Marketplace** and enable it.
- Click on the **CREDENTIALS** tab and generate an API key.

### **GenderAPI** ###

- Sign up on [Gender API](https://gender-api.com/en/).
- Navigate to the *Authorization tokens* page and create a new key.

## Amazon Web Services (AWS) ##

Register for an AWS account. You might need to provide billing details. AWS charges are negligible for small to medium size projects as you can leverage AWS's free tier. You can also run Orion locally; AWS S3 is the only hard dependency with respect to AWS.

Running Orion will incur costs as the APIs and AWS are not free.
