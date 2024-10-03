import base64
import pandas as pd
from openai import AzureOpenAI

class OpenAIModel:
    def __init__(self, api_base: str, api_key: str, deployment_name: str, api_version: str):
        self.client = AzureOpenAI(
            api_key=api_key,
            api_version=api_version,
            base_url=f"{api_base}/openai/deployments/{deployment_name}"
        )
        self.deployment_name = 'gpt-4o'

    @staticmethod
    def encode_image(image_path: str) -> str:
        """Encode an image to base64 format."""
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode('utf-8')

    def generate_image_description(self, base64_image: str) -> str:
        """Generate a descriptive summary based on the image data."""
        response = self.client.chat.completions.create(
            model=self.deployment_name,
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {
                    "role": "user", 
                    "content": [
                        {
                            "type": "text",
                            "text": "You are an analytical model designed to provide key insights from charts and graphs. "
                                     "For each chart or graph provided, please generate eight concise insights. "
                                     "Focus on trends, patterns, significant data points, comparisons, anomalies, and overall conclusions."
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{base64_image}"
                            }
                        }
                    ]
                }
            ],
            max_tokens=2500
        )

        if response.choices:
            return response.choices[0].message.content
        else:
            return "No descriptive summary could be generated."
        
    def generate_image_description(self, base64_image: str) -> str:
        """Generate a descriptive summary based on the image data."""
        response = self.client.chat.completions.create(
            model=self.deployment_name,
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {
                    "role": "user", 
                    "content": [
                        {
                            "type": "text",
                            "text": "You are an analytical model designed to provide key insights from charts and graphs. "
                                    "For each chart or graph provided, please generate eight concise insights. "
                                    "Focus on trends, patterns, significant data points, comparisons, anomalies, and overall conclusions etc..."
                                    "Format the response using Markdown with the following structure:\n"
                                    "\n### Insights\n"
                                    "1. **Insights names**: <Insights Descriptions with in 2 lines>\n"
                                    "2. **Insights names**: <Insights Descriptions with in 2 lines>\n"
                                    "3. **Insights names**: <Insights Descriptions with in 2 lines>\n"
                                    "4. **Insights names**: <Insights Descriptions with in 2 lines>\n"
                                    "...\n"
                                    "8. **Conclusion**: <Overall conclusion>\n"
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{base64_image}"
                            }
                        }
                    ]
                }
            ],
            max_tokens=2500
        )

        if response.choices:
            return response.choices[0].message.content
        else:
            return "No descriptive summary could be generated."

#  "Ensure that all points are left-aligned and the same font is used for all text. "

    def explain_risk_asset_identification(self, code_text: str) -> str:
        """Explain the approach for identifying risk assets, including weightage distribution."""
        response = self.client.chat.completions.create(
            model=self.deployment_name,
            messages=[
                {"role": "system", "content": "You are a technical assistant"},
                {"role": "user", "content": f"""Please provide a comprehensive summary of the methodology for identifying risk assets that includes the following key components, with each topic limited to 100 words: \n\n1. **Feature Engineering**: Describe how feature engineering is performed and its significance in the risk assessment process.\n2. **Calculation of Composite Stability Score**: Outline the methods used to calculate the Composite Stability Score.\n3. **Calculation of Z-Score**: Detail the approach for calculating the Z-Score.\n4. **Categorization of Assets Based on Z-Score Scores**: Explain how assets are categorized according to their Z-Score values.\n\nPlease ensure the response omits any coding implementation details:\n\n{code_text}"""}
            ],
            max_tokens=2500
        )

        if response.choices:
            return response.choices[0].message.content
        else:
            return "No explanation could be generated."

    def describe_data_frame_with_model(self, df: pd.DataFrame) -> str:
        """Generate a description of the given DataFrame using a chat model."""
        data_str = df.to_string(index=False)

        prompt = (
            "Please provide a concise and short description in a list of points maximum of 150 words for the following table data, "
            "including insights on its columns and any relevant observations:\n\n"
            f"{data_str}\n\n"
        )

        response = self.client.chat.completions.create(
            model=self.deployment_name,
            messages=[
                {"role": "system", "content": "You are a technical assistant"},
                {"role": "user", "content": prompt}
            ],
            max_tokens=2500
        )

        if response.choices:
            return response.choices[0].message.content
        else:
            return "No description could be generated."


