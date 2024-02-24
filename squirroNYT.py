# Evan Alexander Squirro Python Code

import argparse
import logging
import requests
import time

log = logging.getLogger(__name__)


class NYTimesSource(object):
	"""
	A data loader plugin for the NY Times API.
	"""

	def __init__(self, api_key):
		self.api_key = api_key
		self.base_url = "https://api.nytimes.com/svc/search/v2/articlesearch.json"

	def connect(self, inc_column=None, max_inc_value=None):
		log.debug("Incremental Column: %r", inc_column)
		log.debug("Incremental Last Value: %r", max_inc_value)

	def disconnect(self):
		"""Disconnect from the source."""
		# Nothing to do
		pass

	def flatten_article(self, article):
		"""
		Flattens an article dictionary into a single level dictionary.
		"""
		flat_article = {}
		for key, value in article.items():
			if isinstance(value, dict):
				for sub_key, sub_value in self.flatten_article(value).items():
					flat_article[f"{key}.{sub_key}"] = sub_value
			else:
				flat_article[key] = value
		return flat_article

	def getDataBatch(self, batch_size):
		"""
		Generator - Get data from source in batches.

		:returns: One list for each batch. Each of those is a list of
				  flattened dictionaries.
		"""
		page = 0
		while True:
			params = {
				"api-key": self.api_key,
				"q": "Silicon Valley",
				"page": page,
				"sort": "newest",
				"fl": "web_url,headline,snippet,kicker", 
			}
			response = requests.get(self.base_url, params=params)
			if response.status_code == 200:
				data = response.json()
				articles = data.get("response", {}).get("docs", [])
				if not articles:
					break
				yield [self.flatten_article(article) for article in articles]
				page += 1
			else:
				if response.status_code == 429:
					log.warning("Rate limit exceeded / 429 error. Waiting 12 seconds...")
					time.sleep(12)  #Wait for 12 seconds according to NYT throttling documentation
				else:
					log.error("Failed to fetch data. Status code: %d", response.status_code) #Generic error code
					break

	def getSchema(self):
		"""
		Return the schema of the dataset
		:returns: a List containing the names of the columns retrieved from the
				  source
		"""
		schema = [
			"web_url",
			"headline.main",
			"headline.kicker",
			"snippet",
		]
		return schema


if __name__ == "__main__":
	config = {
		"api_key": "CM93Xx877QHa3ci6lC2bAEZ7xDvs1eLa", #API key
	}
	source = NYTimesSource(config["api_key"])

	# This looks like an argparse dependency - but the Namespace class is just
	# a simple way to create an object holding attributes.
	source.args = argparse.Namespace(**config)

	for idx, batch in enumerate(source.getDataBatch(10)):
		print(f"{idx} Batch of {len(batch)} items")
		for item in batch:
			print(item)
