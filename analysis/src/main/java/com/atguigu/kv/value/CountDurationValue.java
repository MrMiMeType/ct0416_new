package com.atguigu.kv.value;

import com.atguigu.kv.base.BaseValue;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Accessors(chain = true)
public class CountDurationValue extends BaseValue {
    private int duration;
    private int count;

    @Override
    public String toString() {
        return duration + "\t" + count ;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(duration);
        dataOutput.write(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.duration = dataInput.readInt();
        this.count = dataInput.readInt();
    }
}
