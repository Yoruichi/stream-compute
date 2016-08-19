package td.olap.computer.mode;

import java.util.Timer;
import java.util.TimerTask;

public abstract class TimerSpout extends Spout {

    protected long timerInterval;

    @Override
    public int execute() {
        new Timer().scheduleAtFixedRate(new TimerTask(){
            @Override
            public void run() {
                process();
            }}, 0, timerInterval); 
        return 0;
    }

    protected abstract void process();

    public long getTimerInterval() {
        return timerInterval;
    }

    public void setTimerInterval(long timerInterval) {
        this.timerInterval = timerInterval;
    }

}
