import openai
import pandas as pd
from time import sleep
from tqdm import tqdm

# 🔑 My OpenAI API key
openai.api_key = "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"

file_path = "classified_movies.xlsx"
df = pd.read_excel(file_path)

# Filter only Moderate and Major disagreements
df = df[df["Disagreement_Level"].isin(["Moderate", "Major"])]

# Add new columns
for col in ["Phase3_Recommended_Primary", "Phase3_Recommended_Secondary", "Phase3_Justification", "Phase3_Confidence"]:
    if col not in df.columns:
        df[col] = ""

# Define comparison prompt
def qc_evaluate(title, plot, p1, s1, p2, s2):
    prompt = f"""
You are an expert in emotional film classification. Compare two classification attempts for the movie \"{title}\".

Use this emotional taxonomy:
1. Heroic Escapism – Epic quests, moral clarity, vicarious empowerment.
2. Redemptive Grit – Hardship and transformation; characters earn growth.
3. Intelligent Rebellion – Subversion, defiance, clever resistance to norms.
4. Dystopian Catharsis – Collapse, despair, emotional release through bleakness.
5. Comfort Nostalgia – Familiar warmth, innocence, safety, retro tone.
6. Collective Resilience – Groups overcoming hardship together.
7. Dark Empathy – Tragic complexity, moral ambiguity, emotional heaviness.
8. Humorous Escape – Wit, absurdity, levity, comedic relief.

Your goal:
- Evaluate which classification better fits the emotional tone and arc.
- You may use internal knowledge of the film; the plot is secondary.
- If one label overlaps (Moderate), evaluate only the conflicting label.
- Return final recommended Primary, Secondary, a short justification, and confidence (0.0–1.0).

Movie: {title}
Plot (if needed): {plot}

Phase 1: Primary = {p1}, Secondary = {s1}
Phase 2: Primary = {p2}, Secondary = {s2}

Respond only in this format:
Recommended Primary:
Recommended Secondary:
Justification:
Confidence Score:
"""
    try:
        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.4,
        )
        lines = response.choices[0].message.content.strip().splitlines()
        rp = next((l.split(":")[1].strip() for l in lines if "Recommended Primary" in l), "")
        rs = next((l.split(":")[1].strip() for l in lines if "Recommended Secondary" in l), "")
        just = next((l.split(":")[1].strip() for l in lines if "Justification" in l), "")
        conf = next((l.split(":")[1].strip() for l in lines if "Confidence Score" in l), "")
        return rp, rs, just, conf
    except Exception as e:
        print(f"Error: {e}")
        return "", "", "", ""

# Run loop
output_file = "phase3_qc_results.xlsx"
for i, row in tqdm(df.iterrows(), total=len(df)):
    rp, rs, just, conf = qc_evaluate(
        row["Title"],
        row.get("Plot", ""),
        row["Original_Primary"], row["Original_Secondary"],
        row["Primary_New"], row["Secondary_New"]
    )
    df.at[i, "Phase3_Recommended_Primary"] = rp
    df.at[i, "Phase3_Recommended_Secondary"] = rs
    df.at[i, "Phase3_Justification"] = just
    df.at[i, "Phase3_Confidence"] = conf
    sleep(1.5)

    if (i + 1) % 25 == 0:
        df.to_excel(output_file, index=False)

df.to_excel(output_file, index=False)
print("✓ Phase 3 QC complete. Results saved to phase3_qc_results.xlsx")
