OTHER_THEME = "other gaming"


def map_to_canonical_theme(raw_text):
    if raw_text is None:
        return OTHER_THEME

    text = str(raw_text).lower().strip()
    specific_games = (
        (("roblox",), "roblox"),
        (("minecraft",), "minecraft"),
        (("cs2", "counter-strike", "counter strike"), "counter-strike 2"),
        (("valorant",), "valorant"),
        (("fortnite",), "fortnite"),
        (("gta", "grand theft auto"), "gta"),
        (("league of legends",), "league of legends"),
        (("dota",), "dota 2"),
        (("apex",), "apex legends"),
        (("overwatch",), "overwatch 2"),
        (("call of duty", "warzone", "cod"), "call of duty"),
        (("pubg",), "pubg"),
        (("free fire",), "free fire"),
        (("brawl stars",), "brawl stars"),
        (("clash royale",), "clash royale"),
        (("clash of clans",), "clash of clans"),
        (("genshin",), "genshin impact"),
        (("fifa", "fc 25", "ea sports fc"), "sports / fifa"),
        (("nba 2k",), "sports / nba 2k"),
        (("rocket league",), "rocket league"),
        (("rainbow six", "r6"), "rainbow six siege"),
        (("elden ring",), "elden ring"),
        (("dark souls",), "dark souls"),
        (("wukong", "black myth"), "black myth wukong"),
        (("cyberpunk",), "cyberpunk 2077"),
        (("red dead", "rdr2"), "red dead redemption 2"),
        (("skyrim", "elder scrolls"), "skyrim"),
        (("resident evil",), "resident evil"),
        (("fnaf", "five nights"), "five nights at freddys"),
        (("phasmophobia",), "phasmophobia"),
        (("lethal company",), "lethal company"),
        (("among us",), "among us"),
        (("helldivers",), "helldivers 2"),
        (("monster hunter",), "monster hunter wilds"),
        (("mario kart",), "mario kart"),
        (("zelda",), "zelda"),
        (("pokemon",), "pokemon"),
    )
    for needles, theme in specific_games:
        if (theme == "league of legends" and text == "lol") or any(
            needle in text for needle in needles
        ):
            return theme

    categories = (
        (("esports", "e-sports", "competitive", "pro gamer"), "esports"),
        (("survival",), "survival"),
        (("horror", "scp"), "horror"),
        (("battle royale",), "battle royale"),
        (("rpg", "roleplay", "role-play"), "rpg"),
        (("strategy", "tactic"), "strategy game"),
        (("simulator", "simulation"), "simulator game"),
        (("indie",), "indie game"),
        (("open world", "sandbox"), "open world / sandbox"),
        (("anime",), "anime games"),
        (("retro",), "retro gaming"),
        (("mobile",), "mobile gaming"),
        (("vr", "virtual reality"), "vr gaming"),
    )
    for needles, theme in categories:
        if any(needle in text for needle in needles):
            return theme

    content_types = (
        (("walkthrough", "playthrough", "no commentary"), "walkthrough / playthrough"),
        (("speedrun",), "speedrun"),
        (("boss fight",), "boss fight"),
        (("tutorial", "tips", "tricks", "guide", "settings"), "tutorial / guides"),
        (
            ("highlight", "montage", "clip", "short", "top plays", "clutch", "trickshot"),
            "highlights & clips",
        ),
        (("funny", "meme", "fail", "rage", "reaction"), "funny moments & memes"),
        (("stream", "twitch", "livestream"), "livestream / vod"),
        (("review", "news"), "gaming news & reviews"),
        (("challenge",), "gaming challenges"),
        (("co-op", "multiplayer"), "multiplayer / co-op"),
    )
    for needles, theme in content_types:
        if any(needle in text for needle in needles):
            return theme

    if any(needle in text for needle in ("setup", "keyboard", "mouse", "hardware", "aim training")):
        return "hardware & setup"
    if "ai gaming" in text or "unreal engine" in text:
        return "tech & ai gaming"
    if any(needle in text for needle in ("fps", "firstperson", "first-person", "shooter")):
        return "fps"
    return OTHER_THEME
