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
public class ContactDimension extends BaseDimension {
    private String telephone;
    private String name;

    @Override
    public String toString() {
        return telephone + "\t" + name ;
    }


    @Override
    public int compareTo(BaseDimension o) {
        ContactDimension contactDimonsion = (ContactDimension) o;
        return this.getTelephone().compareTo(contactDimonsion.getTelephone());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(telephone);
        dataOutput.writeUTF(name);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.telephone = dataInput.readUTF();
        this.name = dataInput.readUTF();
    }
}
