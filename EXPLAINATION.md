# Pipeline Design — Window Choice and State

## Student
Johnry Christian Paduhilao
#101002576

## Why Session Window?

I went with Scenario C (e-commerce fraud) with window = Session 30 min gap.

The fraud pattern focused here is a behavioral, not time-periodic pattern where a bot or a customer account piles items into their cart during a single visit and then disappears  without any purchases. The thing is, that behavior only makes sense when measured within a visit, not across an arbitrary fixed interval. If I used a tumbling or sliding window, a user’s session might land partly in one window and partly in the next. If the split up happens, neither of the window sees enough activity to trigger the alert so the signal becomes weaker.

A session window solves that cleanly. It groups events by the same user as long as they stay active which I put < 30 minutes of silence, and then closes once they go idle. This behavior can be a real world simulation on how a shopping visit actually works. Spark accumulates the user's full sequence of actions within that visit before making a judgement.

Also to note, the 30-minute gap came from the dataset itself. The Kaggle eCommerce dataset uses 30 minutes of inactivity as the standard definition of an ended session, which is also consistent with how the likes of Google Analytics and most e-commerce platforms segment visits. Matching that convention makes the alerts meaningful with real world implementation rather than artifacts of an arbitrary parameter choice.


## Where the Pipeline Requires State

This pipeline is stateful in two distinct places.

First is the session aggregation itself. When Spark sees a `groupBy(session_window(...), user_id)`, it cannot compute the result for a session until that session is actually over. So it has to buffer every partial aggregate like the running `cart_adds` count and `purchases` count per (user_id, session_start) pair across multiple micro-batches while events keep arriving. That per-session count is live state held in the Spark executor's memory.

The second piece is the watermark. I set .withWatermark("event_time", "5 minutes"), which means Spark will ignore any events that arrive more than 5 minutes later than the newest timestamp it has seen. Addtionally,, In append mode, Spark also won’t output a session until the watermark has moved past that session's end time. This is how Spark knows no more valid events for that session can still show up. Until then, the session stays open in state.

This is why simulate_stream.py writes an extra file with timestamps far in the future: those rows push the watermark forward so Spark finally closes and outputs the real sessions. Without that push, Spark would keep waiting and never print anything.
