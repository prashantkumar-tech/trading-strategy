"""Strategy Builder — define entry and exit rules per symbol."""

import streamlit as st
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dashboard.shared import render_sidebar, get_rules, set_rules

st.set_page_config(page_title="Strategy Builder", layout="wide", page_icon="📋")
st.title("📋 Strategy Builder")

cfg = render_sidebar()
symbol        = cfg["symbol"]
initial_capital = cfg["initial_capital"]
default_pos_pct = cfg["default_pos_pct"]

ENTRY_FIELDS = [
    "close", "open", "high", "low", "ma50", "ma200", "volume",
    "close_prev", "open_prev", "ma50_prev", "ma200_prev",
    "prev_day_close", "bar_minutes",
]
EXIT_FIELDS = ENTRY_FIELDS + ["position_return_pct", "days_held"]
OPS = [">", "<", ">=", "<=", "==", "crosses_above", "crosses_below"]


def condition_row(idx: int, rule_key: str, fields: list) -> dict:
    c1, c2, c3 = st.columns(3)
    with c1:
        left = st.selectbox(f"Left [{idx+1}]", fields,
                             index=fields.index("close"), key=f"{rule_key}_left_{idx}")
    with c2:
        op = st.selectbox(f"Operator [{idx+1}]", OPS, key=f"{rule_key}_op_{idx}")
    with c3:
        right_opts = fields + ["(enter value)"]
        right_sel  = st.selectbox(f"Right [{idx+1}]", right_opts,
                                   index=fields.index("ma50"), key=f"{rule_key}_right_sel_{idx}")
        if right_sel == "(enter value)":
            right = st.text_input(f"Value [{idx+1}]", value="0", key=f"{rule_key}_right_val_{idx}")
        else:
            right = right_sel
    return {"left": left, "op": op, "right": right}


# ── Rule Builder tabs ─────────────────────────────────────────────────────────
st.header(f"Rule Builder — {symbol}")
st.caption("Rules are saved per symbol and carry over to the Backtest page.")

tabs = st.tabs(["Add Entry Rule", "Add Exit Rule", "Presets"])

with tabs[0]:
    st.markdown("**Entry rules** open a new position each day the condition is met. First match wins.")
    col1, col2 = st.columns(2)
    with col1:
        entry_label      = st.text_input("Rule name", value="Entry Rule", key="entry_label")
        entry_pos_pct    = st.slider("Position size (% of portfolio)", 1, 50, default_pos_pct, key="entry_pos_pct")
        st.caption(f"= **${initial_capital * entry_pos_pct / 100:,.0f}** at current capital")
        entry_combinator = st.selectbox("Combine with", ["AND", "OR"], key="entry_combinator")
    with col2:
        entry_num_conds = st.number_input("Conditions", 1, 5, 1, key="entry_num_conds")
    entry_conditions = [condition_row(i, "entry", ENTRY_FIELDS) for i in range(int(entry_num_conds))]
    if st.button("Add Entry Rule"):
        rules = get_rules(symbol)
        rules.append({"type": "entry", "label": entry_label, "combinator": entry_combinator,
                       "position_pct": entry_pos_pct / 100, "conditions": entry_conditions})
        set_rules(symbol, rules)
        st.success(f"Added '{entry_label}' to {symbol}.")

with tabs[1]:
    st.markdown("**Exit rules** are checked for every open position each day. Any match closes the position.")
    col1, col2 = st.columns(2)
    with col1:
        exit_label      = st.text_input("Rule name", value="Exit Rule", key="exit_label")
        exit_combinator = st.selectbox("Combine with", ["AND", "OR"], key="exit_combinator")
    with col2:
        exit_num_conds = st.number_input("Conditions", 1, 5, 1, key="exit_num_conds")
    exit_conditions = [condition_row(i, "exit", EXIT_FIELDS) for i in range(int(exit_num_conds))]
    if st.button("Add Exit Rule"):
        rules = get_rules(symbol)
        rules.append({"type": "exit", "label": exit_label, "combinator": exit_combinator,
                       "conditions": exit_conditions})
        set_rules(symbol, rules)
        st.success(f"Added '{exit_label}' to {symbol}.")

with tabs[2]:
    st.markdown(f"Load a complete strategy for **{symbol}**. Replaces existing rules.")
    pc1, pc2 = st.columns(2)
    with pc1:
        if st.button("MA50 Momentum", use_container_width=True):
            set_rules(symbol, [
                {"type": "entry", "label": "Above MA50 — 10%", "combinator": "AND",
                 "position_pct": 0.10, "conditions": [{"left": "close", "op": ">", "right": "ma50"}]},
                {"type": "entry", "label": "Below MA50 — 5%", "combinator": "AND",
                 "position_pct": 0.05, "conditions": [{"left": "close", "op": "<=", "right": "ma50"}]},
                {"type": "exit", "label": "2% profit target", "combinator": "AND",
                 "conditions": [{"left": "position_return_pct", "op": ">=", "right": "2"}]},
                {"type": "exit", "label": "3-day time stop", "combinator": "AND",
                 "conditions": [{"left": "days_held", "op": ">=", "right": "3"}]},
            ])
            st.success(f"MA50 Momentum loaded for {symbol}.")
            st.rerun()
    with pc2:
        if st.button("Golden Cross / Death Cross", use_container_width=True):
            set_rules(symbol, [
                {"type": "entry", "label": "Golden Cross", "combinator": "AND",
                 "position_pct": 0.10, "conditions": [{"left": "ma50", "op": "crosses_above", "right": "ma200"}]},
                {"type": "exit", "label": "Death Cross", "combinator": "AND",
                 "conditions": [{"left": "ma50", "op": "crosses_below", "right": "ma200"}]},
            ])
            st.success(f"Golden Cross loaded for {symbol}.")
            st.rerun()
    if st.button("Clear All Rules", type="secondary"):
        set_rules(symbol, [])
        st.rerun()

# ── Active Rules ──────────────────────────────────────────────────────────────
st.divider()
current_rules = get_rules(symbol)

if current_rules:
    st.subheader(f"Active Rules — {symbol}")
    for section_label, rule_type, badge in [("Entry Rules", "entry", "🟢"), ("Exit Rules", "exit", "🔴")]:
        section = [r for r in current_rules if r.get("type") == rule_type]
        if not section:
            continue
        st.markdown(f"**{section_label}**")
        for rule in section:
            i = current_rules.index(rule)
            cond_strs = [f"`{c['left']}` **{c['op']}** `{c['right']}`" for c in rule["conditions"]]
            joined    = f" _{rule['combinator']}_ ".join(cond_strs)
            is_entry  = rule_type == "entry"

            if is_entry:
                pct = rule["position_pct"]
                col_r, col_edit, col_del = st.columns([5, 1, 1])
                with col_r:
                    st.markdown(f"{badge} **{rule['label']}** — **{pct*100:.0f}% = ${initial_capital*pct:,.0f}** — {joined}")
                with col_edit:
                    if st.button("Edit %", key=f"edit_{symbol}_{i}"):
                        st.session_state[f"editing_pct_{symbol}_{i}"] = True
                with col_del:
                    if st.button("Remove", key=f"del_{symbol}_{i}"):
                        current_rules.pop(i); set_rules(symbol, current_rules); st.rerun()

                if st.session_state.get(f"editing_pct_{symbol}_{i}"):
                    new_pct = st.slider(f"New % for '{rule['label']}'", 1, 50,
                                         int(pct * 100), key=f"new_pct_{symbol}_{i}")
                    st.caption(f"= **${initial_capital * new_pct / 100:,.0f}** now · **${15_000 * new_pct / 100:,.0f}** at $15k")
                    sc1, sc2 = st.columns([1, 5])
                    with sc1:
                        if st.button("Save", key=f"save_pct_{symbol}_{i}", type="primary"):
                            current_rules[i]["position_pct"] = new_pct / 100
                            set_rules(symbol, current_rules)
                            del st.session_state[f"editing_pct_{symbol}_{i}"]
                            st.rerun()
                    with sc2:
                        if st.button("Cancel", key=f"cancel_pct_{symbol}_{i}"):
                            del st.session_state[f"editing_pct_{symbol}_{i}"]; st.rerun()
            else:
                col_r, col_del = st.columns([6, 1])
                with col_r:
                    st.markdown(f"{badge} **{rule['label']}** — {joined}")
                with col_del:
                    if st.button("Remove", key=f"del_{symbol}_{i}"):
                        current_rules.pop(i); set_rules(symbol, current_rules); st.rerun()
else:
    st.info(f"No rules for {symbol} yet. Use the tabs above or load a preset.")
