
# About

The dataset is a public dataset from Kaggle: [Ecommerce Events History in Cosmetics Shop](https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop). Source of the data is [REES46 Marketing Platform](https://rees46.com/).

It contains behavior data for 5 months (Oct 2019 – Feb 2020) from a medium cosmetics online store.

Each row in the file represents an event. All events are related to products and users. Each event is like many-to-many relation between products and users.

Data collected by Open CDP project. Feel free to use open source customer data platform.

# How to read it

There are different types of events. See below.

Semantics (or how to read it):

> User **user_id** during session **user_session** added to shopping cart (property **event_type** is equal **cart**) product **product_id** of brand **brand** of category **category_id** (**category_code**) with price **price** at **event_time**.

**File structure**

| **Property**      | **Description**                                                                                                                                                  |
|-------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **event_time**    | Time when event happened at (in UTC).                                                                                                                            |
| **event_type**    | Type of an event (view, cart, remove_from_cart, purchase).                                                                                                       |
| **product_id**    | ID of a product                                                                                                                                                  |
| **category_id**   | Product's category ID                                                                                                                                            |
| **category_code** | Product's category taxonomy (code name) if it was possible to make it. Usually present for meaningful categories and skipped for different kinds of accessories. |
| **brand**         | Downcased string of brand name. Can be missed.                                                                                                                   |
| **price**         | Float price of a product. Present.                                                                                                                               |
| **user_id**       | Permanent user ID.                                                                                                                                               |
| **user_session**  | Temporary user's session ID. Same for each user's session. Is changed every time user come back to online store from a long pause.                               |

**Event types**

Events can be:

* `view` - a user viewed a product
* `cart` - a user added a product to shopping cart
* `remove_from_cart` - a user removed a product from shopping cart
* `purchase` - a user purchased a product

**Multiple purchases per session**

A session can have multiple purchase events. It's ok, because it's a single order.