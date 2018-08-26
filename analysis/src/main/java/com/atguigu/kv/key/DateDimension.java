package com.atguigu.kv.key;

import com.atguigu.kv.base.BaseDimension;
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
public class DateDimension extends BaseDimension {
    private String year;
    private String month;
    private String day;
    @Override
    public int compareTo(BaseDimension o) {
        DateDimension dateDimonsion = (DateDimension) o;
        int result = 0;
        result = this.getYear().compareTo(dateDimonsion.getYear());
        if(result ==0){
            result = this.getMonth().compareTo(dateDimonsion.getMonth());
            if (result ==0){
                result = this.getDay().compareTo(dateDimonsion.getDay());
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return year + '\t' + month + '\t' + day ;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(year);
        dataOutput.writeUTF(month);
        dataOutput.writeUTF(day);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year = dataInput.readUTF();
        this.month = dataInput.readUTF();
        this.day = dataInput.readUTF();
    }
}
