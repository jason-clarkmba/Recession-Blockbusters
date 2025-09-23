# needs_rationale_v4.py

import glob
import os
import logging
import pandas as pd
from time import sleep
from tqdm import tqdm
import openai

# 🔑 Set your API key (or export OPENAI_API_KEY in your environment)
openai.api_key = "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"



# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

print("OK")  # sanity check

# 1) Auto-detect the input Excel file
candidates = glob.glob("needs_r*.xls*")
if not candidates:
    raise FileNotFoundError("No file matching needs_r*.xls* found in current directory")
file_path = candidates[0]
logging.info(f"Loading input file: {file_path}")

# 2) Load the workbook
df = pd.read_excel(file_path)

# 3) Ensure the rationale column exists
rationale_col = "Classification Rationale (AI QC)"
if rationale_col not in df.columns:
    df[rationale_col] = ""

# 4) Prepare token usage tracking
total_input_tokens = 0
total_output_tokens = 0

# 5) Define the QC prompt function with attribute access
def fill_rationale(title, qc3_primary, qc3_secondary, qc2_primary):
    prompt = f"""
You are a film classification QC assistant.
The AI has chosen for "{title}":
  • Phase 3 Primary Classification: {qc3_primary}
  • Phase 3 Secondary Classification: {qc3_secondary}
  • Phase 2 Primary Classification: {qc2_primary}

Provide a concise justification (1–2 sentences) explaining why these labels fit the movie’s emotional tone.
Respond with just the rationale text.
"""
    resp = openai.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.4,
    )
    # use attributes instead of dict subscripting
    usage = resp.usage
    text = resp.choices[0].message.content.strip()
    return text, usage.prompt_tokens, usage.completion_tokens

# 6) Iterate through all rows, regenerating rationale each time
for idx, row in tqdm(df.iterrows(), total=len(df), desc="QC pass 3.5"):
    title = str(row["Title"]).strip().strip('"').strip("'")
    qc3_p = row.get("QC Primary Classification", "")
    qc3_s = row.get("QC Secondary Classification", "")
    qc2_p = row.get("Primary Peer AI check", "")
    try:
        rationale, inp_toks, out_toks = fill_rationale(title, qc3_p, qc3_s, qc2_p)
        df.at[idx, rationale_col] = rationale
        total_input_tokens += inp_toks
        total_output_tokens += out_toks
    except Exception as e:
        logging.error(f"Error on '{title}': {e}")
    sleep(1.0)

    if (idx + 1) % 25 == 0:
        interim = "needs_rationale_progress.xlsx"
        df.to_excel(interim, index=False)
        logging.info(f"Saved interim progress to {interim}")

# 7) Save final results
output_file = "needs_rationale_filled_v5.xlsx"
df.to_excel(output_file, index=False)
logging.info(f"✓ Classification complete. Saved to {output_file}")

# 💰 Show token usage and estimated cost
input_token_cost = 0.03
output_token_cost = 0.06
total_tokens = total_input_tokens + total_output_tokens
input_cost = (total_input_tokens / 1000) * input_token_cost
output_cost = (total_output_tokens / 1000) * output_token_cost

print(f"Total tokens used: {total_tokens}")
print(f"Estimated cost: ${(input_cost + output_cost):.4f}")
