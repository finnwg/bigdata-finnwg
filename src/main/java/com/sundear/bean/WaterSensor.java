package com.sundear.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor

public class WaterSensor {
    private String id;
    private Long ts;
    private Integer vc;
}
