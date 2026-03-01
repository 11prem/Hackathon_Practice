
# LARGE RETAIL ERRONEOUS DATASET (Hackathon Practice)

## Dataset Size
- customers.csv → 1500+ rows
- products.csv → 500+ rows
- sales.csv → 5000+ rows

---

## Errors Injected

### Customers
- Duplicate customer_id rows
- Missing customer_id values
- Malformed and blank emails
- Invalid phone numbers (letters, blanks, mixed formats)
- Mixed date formats
- Trailing spaces in city names
- Invalid postal codes (letters included)
- Mixed gender formats and blanks
- Birthdates in wrong formats

### Products
- Missing product_id
- Missing product_name
- Currency symbols in price
- Negative prices
- Zero prices
- "free" as price
- Cost stored as string
- cost > price
- Mixed category naming
- Mixed date formats
- Invalid discontinued flags
- Completely malformed row

### Sales
- Duplicate sale_id
- Invalid foreign keys (customer_id/product_id not existing)
- Quantity as text ("one")
- Zero and negative quantity
- Currency symbols in unit_price
- Incorrect total_amount calculation
- Mixed date formats
- Inconsistent payment_type casing
- Negative discounts
- Discount as percentage string
- Blank store_id
- Corrupted row without proper columns

---

## Practice Goals
- Schema validation
- Type casting & normalization
- Date standardization
- Referential integrity validation
- Duplicate detection
- Outlier handling
- Data quality reporting
- Logging bad rows

This dataset mimics real hackathon panel-style messy exports.
