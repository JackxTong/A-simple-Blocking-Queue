1. extract_swap_sdr_kdb_data.py

Changes: * Removed unused columns from the RFQ query (riskOwnerBook, clientName, legCoverPrice, parentCoverPrice) to speed up extraction.

Stripped the update statements from the SDR query (except numLegs aggregation). We now extract raw data and handle the nulls and string manipulations in Python.

2. sdr_data_processing.py
Changes: * Added extract_sdr_logic_from_raw_data to handle the pricing logic, string concatenations, and NaN fills that we stripped from the q query.


3. matching_algorithm.py
Changes:

Removed all the tracking counters (self.all_correct_rfq_matches, etc.) from the loop.

Built a new calculate_match_statistics method that calculates percentages accurately using the final MatchAttributes.MATCH.value column, running after all parent-level checks and unmatched logic finishes.


The RFQ Matching Journey
When the algorithm picks up a specific requestId, it extracts three key pieces of context: the numLegs, the legPricingConvention (e.g., RateQuoted vs. NPV), and whether it is a MAC trade (by checking if "MAC" is in the legInstrumentName).

Step 1: Dynamic Feature Selection
The algorithm establishes a baseline list of columns that must match perfectly between the RFQ and SDR tables:

tradeDate

numLegs (only required if > 1)

legInstrumentMaturityDate

legSwapEffectiveDate

leg_SwapFixedLegPayFrequency

leg_SwapFloatingLegPayFrequency

Special Handling (NPV/MAC): If the legPricingConvention is "NPV", it calls apply_mac_and_npv_changes(). This function dynamically alters the required feature list. For example, MAC (Market Agreed Coupon) trades have standardized IMM dates, so the algorithm often loosens or modifies the strict maturity/effective date matching rules for these specific trades.

Step 2: The Leg-by-Leg Gauntlet
For each leg of the RFQ, the SDR table is filtered down through four sequential gates:

The Instrument Gate (Exact Match): * Filters SDR data to match the RFQ on the dynamic feature list defined in Step 1.

The Time Gate (Range Match): * Filters SDR sdrExecutionTimestamp to fall strictly between [firstQuoteTime - 5 seconds, lastQuoteTime + 10 seconds].

The Price Gate (Tolerance Match - Conditional):

If traded with Barclays & RateQuoted: SDR sdrLegPrice must be within tolerance of RFQ legQuotePrice.

If traded with Barclays & NPV: SDR PackageTransactionPrice must be within tolerance of RFQ parentQuotePrice.

If traded away & NPV: Simply ensures SDR PackageTransactionPrice is not null.

The Notional Gate (Tolerance Match - Conditional):

If NOT NPV: SDR sdrSize must be within a percentage tolerance of the RFQ's cappedLegSize.

Step 3: Tie-Breaking & Reverse Validation
If multiple SDR rows survive the gauntlet, the code executes a tie-breaker.

Tie-Breaker: It selects the SDR row with the sdrExecutionTimestamp closest to the RFQ's lastQuoteTime (classify_sdr_entry_based_on_matching).

Reverse Check: It takes that winning SDR row and runs it backwards against the RFQ table (classify_rfq_entry_based_on_matching) to ensure this SDR entry doesn't accidentally map perfectly to a different RFQ.

Step 4: State Management & Unmatching
Dissemination ID Lockout: If the leg is fully matched and the trade was executed with Barclays, the algorithm drops that specific sdrDisseminationId from the main SDR pool so it cannot be matched to the next leg of the RFQ.

Post-Loop Unmatching: Even if an RFQ gets matched, it goes through a final parent-level audit. If the absolute difference between parentMidFromPriceTrace and sdrParentPrice is > 0.35 bps (or the NPV equivalent using parentDv01), or if the sdrParentPrice is NaN, the match is retroactively revoked.