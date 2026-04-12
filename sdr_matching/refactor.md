1. extract_swap_sdr_kdb_data.py

Changes: * Removed unused columns from the RFQ query (riskOwnerBook, clientName, legCoverPrice, parentCoverPrice) to speed up extraction.

Stripped the update statements from the SDR query (except numLegs aggregation). We now extract raw data and handle the nulls and string manipulations in Python.

2. sdr_data_processing.py
Changes: * Added extract_sdr_logic_from_raw_data to handle the pricing logic, string concatenations, and NaN fills that we stripped from the q query.


3. matching_algorithm.py
Changes:

Removed all the tracking counters (self.all_correct_rfq_matches, etc.) from the loop.

Built a new calculate_match_statistics method that calculates percentages accurately using the final MatchAttributes.MATCH.value column, running after all parent-level checks and unmatched logic finishes.