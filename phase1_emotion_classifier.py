
import openai
import pandas as pd
from time import sleep
from tqdm import tqdm

# 🔑 My OpenAI API key
openai.api_key = "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"

# 📂 Load Excel with explicit sheet name
file_path = "master_movie_data.xlsx"
df = pd.read_excel(file_path, sheet_name="Movie-data_2000-2023")

# ➕ Add columns for results
if "Primary_New" not in df.columns:
    df["Primary_New"] = ""
    df["Secondary_New"] = ""
    df["Confidence"] = ""
    df["Rationale"] = ""
    df["Disagrees_With_Original"] = ""
    df["Original_Primary"] = df["Primary"] if "Primary" in df.columns else ""
    df["Original_Secondary"] = df["Secondary"] if "Secondary" in df.columns else ""

# 💰 Initialize token and cost counters
total_tokens = 0
input_token_cost = 0.005  # gpt-4o pricing
output_token_cost = 0.015
total_input_tokens = 0
total_output_tokens = 0

# 🧠 Define full emotional taxonomy prompt
def classify_emotion(title, plot):
    global total_tokens, total_input_tokens, total_output_tokens

    prompt = f"""You are an expert film classifier. Assign emotional categories to the film \"{title}\" using the emotional taxonomy below. Use the plot or genre to guide classification. Base your decision on emotional tone, narrative arc, and psychological resonance.

Taxonomy:
1. Heroic Escapism – Big, bold stories that sweep you out of everyday life: epic quests, daring heroes, impossible odds. These deliver clarity, courage, and hope — vicarious strength when reality feels uncertain.

2. Redemptive Grit – Grounded, personal stories of hardship and transformation. A character earns growth through humility, endurance, or reckoning. Uplifting, but through effort. Resonates deeply in hard times.

3. Intelligent Rebellion – Stories that challenge authority or societal norms with wit, insight, or subversion. These may be political thrillers, dark satires, or clever dramas with rebellious protagonists.

4. Dystopian Catharsis – Emotional release through collapse, corruption, or existential despair. Must show broken systems, emotional impact, and cathartic or futile tone. It’s not just bleak — it confronts our collective fears.

5. Comfort Nostalgia – Emotional warmth rooted in familiarity. Not just “retro,” but cozy rhythms and innocence that evoke safety, family, or “simpler times.” Aimed at emotional comfort, not period accuracy.

6. Collective Resilience – Group efforts against adversity. A team, town, family, or movement that survives or triumphs together. Emphasizes shared strength, solidarity, and belonging.

7. Dark Empathy – We feel with suffering, flawed, or tragic characters. This isn’t just darkness — it’s emotional complexity and moral nuance. Stories often end unresolved or bittersweet.

8. Humorous Escape – Comedies that bring relief, playfulness, and absurdity. Whether goofy or clever, their emotional appeal is laughter and levity — an antidote to stress.

Return ONLY this format:
Primary Emotion:
Secondary Emotion:
Confidence Score (0.0–1.0):
Short Rationale:

Plot or Genre:
{plot}
"""

    try:
        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.4,
        )
        content = response.choices[0].message.content.strip()
        usage = response.usage
        total_input_tokens += usage.prompt_tokens
        total_output_tokens += usage.completion_tokens
        total_tokens += usage.total_tokens

        lines = content.splitlines()
        primary = next((l.split(":")[1].strip() for l in lines if "Primary" in l), "")
        secondary = next((l.split(":")[1].strip() for l in lines if "Secondary" in l), "")
        confidence = next((l.split(":")[1].strip() for l in lines if "Confidence" in l), "")
        rationale = next((l.split(":")[1].strip() for l in lines if "Rationale" in l), "")
        return primary, secondary, confidence, rationale

    except Exception as e:
        print(f"Error: {e}")
        return "", "", "", ""

# 🚀 Run classification loop
output_file = "classified_movies.xlsx"
save_interval = 50  # Save after every 50 rows

for i, row in tqdm(df.iterrows(), total=len(df)):
    primary, secondary, conf, rationale = classify_emotion(row["Title"], row.get("Plot", row.get("Genre", "")))
    df.at[i, "Primary_New"] = primary
    df.at[i, "Secondary_New"] = secondary
    df.at[i, "Confidence"] = conf
    df.at[i, "Rationale"] = rationale
    if "Primary" in df.columns and "Secondary" in df.columns:
        disagreement = primary != row["Primary"] or secondary != row["Secondary"]
        df.at[i, "Disagrees_With_Original"] = "Yes" if disagreement else "No"
    sleep(1.5)

    # 💾 Periodic save
    if (i + 1) % save_interval == 0:
        df.to_excel(output_file, index=False)

# 💾 Final save
df.to_excel(output_file, index=False)

# 💰 Show token usage and estimated cost
input_cost = (total_input_tokens / 1000) * input_token_cost
output_cost = (total_output_tokens / 1000) * output_token_cost
print(f"✓ Classification complete. Saved to {output_file}")
print(f"Total tokens used: {total_tokens}")
print(f"Estimated cost: ${(input_cost + output_cost):.4f}")
