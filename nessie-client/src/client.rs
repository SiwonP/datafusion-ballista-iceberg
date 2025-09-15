
use crate::error::NessieError;
use crate::models::{
    CommitResponse, EntriesResponse, Reference, ReferenceResponse,
    ReferencesResponse,
};
use reqwest::Client;
use url::Url;



#[derive(Debug)]
pub struct NessieClient {
    base_url: Url,
    client: Client,
}

impl NessieClient {
    pub fn new(base_url: &str) -> Result<Self, NessieError> {
        Ok(Self {
            base_url: base_url.parse()?,
            client: Client::new(),
        })
    }

    pub async fn list_references(&self) -> Result<Vec<Reference>, NessieError> {
        let url = self.base_url.join("trees")?;
        // let response = self.client.get(url).send().await?.json::<ReferencesResponse>()?;
        let response = self
            .client
            .get(url)
            .send()
            .await?
            .json::<ReferencesResponse>()
            .await?;
        Ok(response.references)
    }

    pub async fn get_reference(
        &self,
        reference_name: String,
    ) -> Result<ReferenceResponse, NessieError> {
        let url = self.base_url.join(&format!("trees/{}", reference_name))?;
        let response = self
            .client
            .get(url)
            .send()
            .await?
            .json::<ReferenceResponse>()
            .await?;
        Ok(response)
    }

    pub async fn create_reference(
        &self,
        name: String,
        ref_type: String,
        reference: Reference,
    ) -> Result<ReferenceResponse, NessieError> {
        let url = self.base_url.join("trees")?;
        let response = self
            .client
            .post(url)
            .query(&[("name", name), ("type", ref_type)])
            .json(&reference)
            .send()
            .await?
            .json::<ReferenceResponse>()
            .await?;
        Ok(response)
    }

    pub async fn delete_reference(
        &self,
        reference: Reference,
    ) -> Result<ReferenceResponse, NessieError> {
        let url = self.base_url.join(&format!("trees/{}", reference.name))?;
        let response = self
            .client
            .delete(url)
            .send()
            .await?
            .json::<ReferenceResponse>()
            .await?;
        Ok(response)
    }

    pub async fn list_entries(&self, reference: &str) -> Result<Vec<String>, NessieError> {
        let url = self
            .base_url
            .join(&format!("trees/{}/entries", reference))?;
        let response = self.client.get(url).send().await?;

        // if !response.status().is_success() {
        //     let status = response.status();
        //     let body = response.text().unwrap_or_default();
        //     return Err(NessieError::InvalidResponse(format!(
        //         "Status: {}, Body: {}",
        //         status, body
        //     )));
        // }

        let parsed: EntriesResponse = response.json().await?;

        let entries = parsed
            .entries
            .into_iter()
            .map(|e| e.name.to_string())
            .collect();

        Ok(entries)
    }

    pub async fn commit_entry(
        &self,
        branch: &str,
        operations: crate::models::Operations,
    ) -> Result<crate::models::CommitResponse, NessieError> {
        let url = self
            .base_url
            .join(&format!("trees/{}/history/commit", branch))?;
        let response = self.client.post(url).json(&operations).send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<failed to read body>".to_string());
            println!("{}", status);
            println!("{}", body);
            return Err(NessieError::InvalidResponse(format!(
                "HTTP {}: {}",
                status, body
            )));
        }
        Ok(response.json::<CommitResponse>().await?)
    }
}
