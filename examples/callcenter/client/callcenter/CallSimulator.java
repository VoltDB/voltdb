/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package callcenter;

import java.util.ArrayDeque;
import java.util.Date;
import java.util.Queue;
import java.util.Random;

/**
 * Generator for fake call data.
 *
 * Generates pairs of events, begin calls and end calls.
 *
 * End calls are scheduled for future delivery.
 *
 */
public class CallSimulator implements EventSource<CallEvent> {

    CallCenterApp.CallCenterConfig config;

    // random number generator with constant seed
    final Random rand = new Random(0);

    final DelayedQueue<CallEvent> delayedEvents = new DelayedQueue<>();

    // used for pacing
    long currentSystemMilliTimestamp = 0;
    double targetEventsPerMillisecond;
    long targetEventsThisMillisecond;
    long eventsSoFarThisMillisecond;

    // incrementing counter
    long lastCallIdUsed = 0;

    Queue<Integer> agentsAvailable = new ArrayDeque<>();
    Queue<Long> phoneNumbersAvailable = new ArrayDeque<>();

    CallSimulator(CallCenterApp.CallCenterConfig config) {
        this.config = config;
        targetEventsPerMillisecond = 5;

        // generate agents
        for (int i = 0; i < config.agents; i++) {
            agentsAvailable.add(i);
        }

        // generate phone numbers
        for (int i = 0; i < config.numbers; i++) {
            // random area code between 200 and 799
            long areaCode = rand.nextInt(600) + 200;
            // random exchange between 200 and 999
            long exhange = rand.nextInt(800) + 200;
            // full random number
            long phoneNo = areaCode * 10000000 + exhange * 10000 + rand.nextInt(9999);
            phoneNumbersAvailable.add(phoneNo);
        }
    }

    /**
     * Generate a random call event with a duration.
     *
     * Reserves agent and phone number from the pool.
     */
    CallEvent[] makeRandomEvent() {
        long callId = ++lastCallIdUsed;

        // get agentid
        Integer agentId = agentsAvailable.poll();
        if (agentId == null) {
            return null;
        }

        // get phone number
        Long phoneNo = phoneNumbersAvailable.poll();
        assert(phoneNo != null);

        // voltdb timestamp type uses micros from epoch
        Date startTS = new Date(currentSystemMilliTimestamp);

        long durationms = -1;
        long meancalldurationms = config.meancalldurationseconds * 1000;
        long maxcalldurationms = config.maxcalldurationseconds * 1000;
        double stddev = meancalldurationms / 2.0;

        // repeat until in the range (0..maxcalldurationms]
        while ((durationms <= 0) || (durationms > maxcalldurationms)) {
            durationms = (long) (rand.nextGaussian() * stddev) + meancalldurationms;
        }
        Date endTS = new Date(startTS.getTime() + durationms);

        CallEvent[] event = new CallEvent[2];
        event[0] = new CallEvent(callId, agentId, phoneNo, startTS, null);
        event[1] = new CallEvent(callId, agentId, phoneNo, null, endTS);

        // some debugging code
        //System.out.println("Creating event with range:");
        //System.out.println(new Date(startTS.getTime() / 1000));
        //System.out.println(new Date(endTS.getTime() / 1000));

        return event;
    }

    /**
     * Return the next call event that is safe for delivery or null
     * if there are no safe objects to deliver.
     *
     * Null response could mean empty, or could mean all objects
     * are scheduled for the future.
     *
     * @param systemCurrentTimeMillis The current time.
     * @return CallEvent
     */
    @Override
    public CallEvent next(long systemCurrentTimeMillis) {
        // check for time passing
        if (systemCurrentTimeMillis > currentSystemMilliTimestamp) {
            // build a target for this 1ms window
            long eventBacklog = targetEventsThisMillisecond - eventsSoFarThisMillisecond;
            targetEventsThisMillisecond = (long) Math.floor(targetEventsPerMillisecond);
            double targetFraction = targetEventsPerMillisecond - targetEventsThisMillisecond;
            targetEventsThisMillisecond += (rand.nextDouble() <= targetFraction) ? 1 : 0;
            targetEventsThisMillisecond += eventBacklog;
            // reset counter for this 1ms window
            eventsSoFarThisMillisecond = 0;
            currentSystemMilliTimestamp = systemCurrentTimeMillis;
        }

        // drain scheduled events first
        CallEvent callEvent = delayedEvents.nextReady(systemCurrentTimeMillis);
        if (callEvent != null) {
            // double check this is an end event
            assert(callEvent.startTS == null);
            assert(callEvent.endTS != null);

            // return the agent/phone for this event to the available lists
            agentsAvailable.add(callEvent.agentId);
            phoneNumbersAvailable.add(callEvent.phoneNo);

            validate();
            return callEvent;
        }

        // check if we made all the target events for this 1ms window
        if (targetEventsThisMillisecond == eventsSoFarThisMillisecond) {
            validate();
            return null;
        }

        // generate rando event (begin/end pair)
        CallEvent[] event = makeRandomEvent();
        // this means all agents are busy
        if (event == null) {
            validate();
            return null;
        }

        // schedule the end event
        long endTimeKey = event[1].endTS.getTime();
        assert((endTimeKey - systemCurrentTimeMillis) < (config.maxcalldurationseconds * 1000));
        delayedEvents.add(endTimeKey, event[1]);

        eventsSoFarThisMillisecond++;

        validate();
        return event[0];
    }

    /**
     * Ignore any scheduled delays and return events in
     * schedule order until empty.
     */
    @Override
    public CallEvent drain() {
        CallEvent callEvent = delayedEvents.drain();
        if (callEvent == null) {
            validate();
            return null;
        }

        // double check this is an end event
        assert(callEvent.startTS == null);
        assert(callEvent.endTS != null);

        // return the agent/phone for this event to the available lists
        agentsAvailable.add(callEvent.agentId);
        phoneNumbersAvailable.add(callEvent.phoneNo);

        validate();
        return callEvent;
    }

    /**
     * Smoke check on validity of data structures.
     * This was useful while getting the code right for this class,
     * but it doesn't do much now, unless the code needs changes.
     */
    private void validate() {
        long delayedEventCount = delayedEvents.size();

        long outstandingAgents = config.agents - agentsAvailable.size();
        long outstandingPhones = config.numbers - phoneNumbersAvailable.size();

        if (outstandingAgents != outstandingPhones) {
            throw new RuntimeException(
                    String.format("outstandingAgents (%d) != outstandingPhones (%d)",
                            outstandingAgents, outstandingPhones));
        }
        if (outstandingAgents != delayedEventCount) {
            throw new RuntimeException(
                    String.format("outstandingAgents (%d) != delayedEventCount (%d)",
                            outstandingAgents, delayedEventCount));
        }
    }

    /**
     * Debug statement to help users verify there are no lost or delayed events.
     */
    void printSummary() {
        System.out.printf("There are %d agents outstanding and %d phones. %d entries waiting to go.\n",
                agentsAvailable.size(), phoneNumbersAvailable.size(), delayedEvents.size());
    }
}
