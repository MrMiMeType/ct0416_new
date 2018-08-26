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
public class CommonDimension extends BaseDimension {
    private ContactDimension contactDimonsion = new ContactDimension();
    private DateDimension dateDimonsion = new DateDimension();

    @Override
    public int compareTo(BaseDimension o) {
        CommonDimension commonDimonsion = (CommonDimension) o;
        int result = 0;
        result = this.getContactDimonsion().compareTo(commonDimonsion.getContactDimonsion());
        if (result==0) {
            result = this.getDateDimonsion().compareTo(commonDimonsion.getDateDimonsion());
        }
        return result;
    }

    @Override
    public String toString() {
        return contactDimonsion + "\t" + dateDimonsion;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.contactDimonsion.write(dataOutput);
        this.dateDimonsion.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.contactDimonsion.readFields(dataInput);
        this.dateDimonsion.readFields(dataInput);
    }
}
